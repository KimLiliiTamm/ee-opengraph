"""Äriregister (Estonian Business Registry) ETL pipeline.

Downloads bulk open-data ZIP files from the registry and ingests:
  - Company nodes (registrikood as primary key)
  - Person nodes (full_name + name_normalized; no isikukood in bulk data since Nov 2024)
  - BOARD_MEMBER_OF, SHAREHOLDER_OF, BENEFICIAL_OWNER_OF relationships

Usage:
    python -m etl.ariregister.pipeline

Environment variables (loaded from .env):
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    ETL_BATCH_SIZE   (default 500)
    ETL_LOG_LEVEL    (default INFO)
"""

import asyncio
import io
import json
import logging
import os
import unicodedata
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from etl.common.http_client import AsyncHttpClient
from etl.common.neo4j_client import Neo4jClient

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------

ARIREGISTER_BASE = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed"

SOURCES = {
    "companies": f"{ARIREGISTER_BASE}/ettevotja_rekvisiidid__lihtandmed.csv.zip",
    "persons": f"{ARIREGISTER_BASE}/ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip",
    "shareholders": f"{ARIREGISTER_BASE}/ettevotja_rekvisiidid__osanikud.json.zip",
    "beneficial_owners": f"{ARIREGISTER_BASE}/ettevotja_rekvisiidid__kasusaajad.json.zip",
}

DATA_DIR = Path("data/ariregister")
BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "500"))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def normalize_name(name: str) -> str:
    """Lowercase + strip diacritics for fuzzy matching.

    Args:
        name: Raw name string.

    Returns:
        ASCII-folded, lowercased name.
    """
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return ascii_str.lower().strip()


def _open_zip_member(zip_path: Path, suffix: str) -> io.BytesIO:
    """Extract the first file with the given suffix from a ZIP archive.

    Args:
        zip_path: Path to the ZIP file.
        suffix: File extension to look for (e.g. ".csv", ".json").

    Returns:
        In-memory buffer of the extracted file contents.
    """
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(suffix)]
        if not names:
            raise ValueError(f"No *{suffix} file found in {zip_path}")
        return io.BytesIO(zf.read(names[0]))


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


async def download_all(client: AsyncHttpClient) -> dict[str, Path]:
    """Download all bulk ZIP files to DATA_DIR.

    Args:
        client: Configured async HTTP client.

    Returns:
        Mapping of dataset name → local ZIP path.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    tasks = {
        name: client.download_file(url, DATA_DIR / f"{name}.zip")
        for name, url in SOURCES.items()
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    for name, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            logger.error("Failed to download %s: %s", name, result)
        else:
            paths[name] = result
    return paths


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_companies(zip_path: Path) -> pd.DataFrame:
    """Parse the company CSV from its ZIP.

    Expected columns (subset used):
        ärinimi, äriregistri kood, õiguslik vorm, KMKR kood,
        staatus, registreerimise kpv, aadress

    Args:
        zip_path: Path to the companies ZIP file.

    Returns:
        DataFrame with normalised column names.
    """
    buf = _open_zip_member(zip_path, ".csv")
    df = pd.read_csv(buf, sep=";", dtype=str, low_memory=False)
    # Normalise to predictable column names regardless of header capitalisation
    df.columns = [c.strip().lower() for c in df.columns]

    rename = {
        "ärinimi": "name",
        "äriregistri kood": "registrikood",
        "õiguslik vorm": "legal_form",
        "kmkr kood": "kmkr",
        "staatus": "status",
        "registreerimise kpv": "registration_date",
        "aadress": "address",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    logger.info("Parsed %d company rows", len(df))
    return df


def parse_persons(zip_path: Path) -> list[dict[str, Any]]:
    """Parse the board-members/persons JSON from its ZIP.

    The JSON is a list of company objects, each containing a ``isikud`` array
    (persons associated with the company).

    Args:
        zip_path: Path to the persons ZIP file.

    Returns:
        Flat list of person-role dicts with ``registrikood`` included.
    """
    buf = _open_zip_member(zip_path, ".json")
    data = json.load(buf)
    records: list[dict[str, Any]] = []
    for company in data:
        registrikood = str(company.get("ariregistri_kood", "")).strip()
        for person in company.get("isikud", []):
            records.append(
                {
                    "registrikood": registrikood,
                    "company_name": company.get("arinimi", ""),
                    "person_name": person.get("nimi", ""),
                    "role": person.get("isiku_roll", ""),
                    "start_date": person.get("algus_kpv", ""),
                    "end_date": person.get("lopp_kpv", ""),
                    "representation": person.get("esindusõigus", ""),
                }
            )
    logger.info("Parsed %d person-role rows", len(records))
    return records


def parse_shareholders(zip_path: Path) -> list[dict[str, Any]]:
    """Parse the shareholders JSON from its ZIP.

    Args:
        zip_path: Path to the shareholders ZIP file.

    Returns:
        Flat list of shareholder dicts with ``registrikood`` included.
    """
    buf = _open_zip_member(zip_path, ".json")
    data = json.load(buf)
    records: list[dict[str, Any]] = []
    for company in data:
        registrikood = str(company.get("ariregistri_kood", "")).strip()
        for sh in company.get("osanikud", []):
            records.append(
                {
                    "registrikood": registrikood,
                    "company_name": company.get("arinimi", ""),
                    "person_name": sh.get("nimi", ""),
                    "role": sh.get("roll", ""),
                    "percentage": sh.get("osalus_protsent", None),
                    "start_date": sh.get("algus_kpv", ""),
                    "end_date": sh.get("lopp_kpv", ""),
                }
            )
    logger.info("Parsed %d shareholder rows", len(records))
    return records


def parse_beneficial_owners(zip_path: Path) -> list[dict[str, Any]]:
    """Parse the beneficial owners JSON from its ZIP.

    Args:
        zip_path: Path to the beneficial owners ZIP file.

    Returns:
        Flat list of beneficial-owner dicts.
    """
    buf = _open_zip_member(zip_path, ".json")
    data = json.load(buf)
    records: list[dict[str, Any]] = []
    for company in data:
        registrikood = str(company.get("ariregistri_kood", "")).strip()
        for ubo in company.get("kasusaajad", []):
            records.append(
                {
                    "registrikood": registrikood,
                    "company_name": company.get("arinimi", ""),
                    "person_name": ubo.get("nimi", ""),
                    "country": ubo.get("elukoha_riik", ""),
                }
            )
    logger.info("Parsed %d beneficial-owner rows", len(records))
    return records


# ---------------------------------------------------------------------------
# Neo4j writers
# ---------------------------------------------------------------------------


def _coerce(val: Any) -> Any:
    """Convert NaN / None to None for Neo4j compatibility."""
    if val is None:
        return None
    try:
        import math
        if math.isnan(float(val)):
            return None
    except (TypeError, ValueError):
        pass
    return val


def ingest_companies(client: Neo4jClient, df: pd.DataFrame) -> None:
    """Merge Company nodes from the companies DataFrame.

    Args:
        client: Connected Neo4j client.
        df: Parsed companies DataFrame.
    """
    cypher = """
    UNWIND $rows AS row
    MERGE (c:Company {registrikood: row.registrikood})
    SET c.name               = row.name,
        c.legal_form         = row.legal_form,
        c.kmkr               = row.kmkr,
        c.status             = row.status,
        c.registration_date  = row.registration_date,
        c.address            = row.address,
        c.source             = ['ariregister'],
        c.last_updated       = datetime()
    """
    rows = []
    for _, row in df.iterrows():
        registrikood = _coerce(row.get("registrikood"))
        if not registrikood:
            continue
        rows.append(
            {
                "registrikood": str(registrikood).strip(),
                "name": _coerce(row.get("name")),
                "legal_form": _coerce(row.get("legal_form")),
                "kmkr": _coerce(row.get("kmkr")),
                "status": _coerce(row.get("status")),
                "registration_date": _coerce(row.get("registration_date")),
                "address": _coerce(row.get("address")),
            }
        )
    count = client.run_query_batch(cypher, rows, batch_size=BATCH_SIZE)
    logger.info("Merged %d Company nodes", count)


def ingest_board_members(client: Neo4jClient, records: list[dict[str, Any]]) -> None:
    """Merge Person nodes and BOARD_MEMBER_OF relationships.

    Note: No isikukood available in bulk data since Nov 2024.
    Person identity key: name_normalized.

    Args:
        client: Connected Neo4j client.
        records: Parsed person-role records.
    """
    cypher = """
    UNWIND $rows AS row
    MERGE (c:Company {registrikood: row.registrikood})
    MERGE (p:Person {name_normalized: row.name_normalized})
    ON CREATE SET
        p.full_name          = row.person_name,
        p.sources            = ['ariregister_bulk'],
        p.confidence         = 'name_only',
        p.needs_enrichment   = true,
        p.first_seen         = datetime()
    SET p.last_updated = datetime()
    MERGE (p)-[r:BOARD_MEMBER_OF {role: row.role, start_date: row.start_date}]->(c)
    SET r.end_date     = row.end_date,
        r.representation = row.representation
    """
    rows = []
    for rec in records:
        name = (rec.get("person_name") or "").strip()
        registrikood = (rec.get("registrikood") or "").strip()
        if not name or not registrikood:
            continue
        rows.append(
            {
                "registrikood": registrikood,
                "person_name": name,
                "name_normalized": normalize_name(name),
                "role": rec.get("role") or "",
                "start_date": rec.get("start_date") or "",
                "end_date": rec.get("end_date") or "",
                "representation": rec.get("representation") or "",
            }
        )
    count = client.run_query_batch(cypher, rows, batch_size=BATCH_SIZE)
    logger.info("Merged %d BOARD_MEMBER_OF relationships", count)


def ingest_shareholders(client: Neo4jClient, records: list[dict[str, Any]]) -> None:
    """Merge Person nodes and SHAREHOLDER_OF relationships.

    Args:
        client: Connected Neo4j client.
        records: Parsed shareholder records.
    """
    cypher = """
    UNWIND $rows AS row
    MERGE (c:Company {registrikood: row.registrikood})
    MERGE (p:Person {name_normalized: row.name_normalized})
    ON CREATE SET
        p.full_name          = row.person_name,
        p.sources            = ['ariregister_bulk'],
        p.confidence         = 'name_only',
        p.needs_enrichment   = true,
        p.first_seen         = datetime()
    SET p.last_updated = datetime()
    MERGE (p)-[r:SHAREHOLDER_OF {start_date: row.start_date}]->(c)
    SET r.end_date    = row.end_date,
        r.percentage  = row.percentage,
        r.role        = row.role
    """
    rows = []
    for rec in records:
        name = (rec.get("person_name") or "").strip()
        registrikood = (rec.get("registrikood") or "").strip()
        if not name or not registrikood:
            continue
        rows.append(
            {
                "registrikood": registrikood,
                "person_name": name,
                "name_normalized": normalize_name(name),
                "role": rec.get("role") or "",
                "percentage": rec.get("percentage"),
                "start_date": rec.get("start_date") or "",
                "end_date": rec.get("end_date") or "",
            }
        )
    count = client.run_query_batch(cypher, rows, batch_size=BATCH_SIZE)
    logger.info("Merged %d SHAREHOLDER_OF relationships", count)


def ingest_beneficial_owners(
    client: Neo4jClient, records: list[dict[str, Any]]
) -> None:
    """Merge Person nodes and BENEFICIAL_OWNER_OF relationships.

    Args:
        client: Connected Neo4j client.
        records: Parsed beneficial-owner records.
    """
    cypher = """
    UNWIND $rows AS row
    MERGE (c:Company {registrikood: row.registrikood})
    MERGE (p:Person {name_normalized: row.name_normalized})
    ON CREATE SET
        p.full_name          = row.person_name,
        p.country            = row.country,
        p.sources            = ['ariregister_bulk'],
        p.confidence         = 'name_only',
        p.needs_enrichment   = true,
        p.first_seen         = datetime()
    SET p.last_updated = datetime()
    MERGE (p)-[r:BENEFICIAL_OWNER_OF]->(c)
    SET r.country = row.country
    """
    rows = []
    for rec in records:
        name = (rec.get("person_name") or "").strip()
        registrikood = (rec.get("registrikood") or "").strip()
        if not name or not registrikood:
            continue
        rows.append(
            {
                "registrikood": registrikood,
                "person_name": name,
                "name_normalized": normalize_name(name),
                "country": rec.get("country") or "",
            }
        )
    count = client.run_query_batch(cypher, rows, batch_size=BATCH_SIZE)
    logger.info("Merged %d BENEFICIAL_OWNER_OF relationships", count)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------


async def run() -> None:
    """Download and ingest all Äriregister bulk datasets."""
    log_level = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    rate_limit = float(os.getenv("ARIREGISTER_RATE_LIMIT", "2"))

    async with AsyncHttpClient(requests_per_second=rate_limit) as http:
        logger.info("Downloading Äriregister bulk files…")
        paths = await download_all(http)

    if not paths:
        logger.error("No files downloaded — aborting")
        return

    with Neo4jClient() as neo4j:
        # Companies
        if "companies" in paths:
            logger.info("Ingesting companies…")
            try:
                df = parse_companies(paths["companies"])
                ingest_companies(neo4j, df)
            except Exception as exc:
                logger.error("Company ingestion failed: %s", exc, exc_info=True)

        # Board members
        if "persons" in paths:
            logger.info("Ingesting board members…")
            try:
                records = parse_persons(paths["persons"])
                ingest_board_members(neo4j, records)
            except Exception as exc:
                logger.error("Board member ingestion failed: %s", exc, exc_info=True)

        # Shareholders
        if "shareholders" in paths:
            logger.info("Ingesting shareholders…")
            try:
                records = parse_shareholders(paths["shareholders"])
                ingest_shareholders(neo4j, records)
            except Exception as exc:
                logger.error("Shareholder ingestion failed: %s", exc, exc_info=True)

        # Beneficial owners
        if "beneficial_owners" in paths:
            logger.info("Ingesting beneficial owners…")
            try:
                records = parse_beneficial_owners(paths["beneficial_owners"])
                ingest_beneficial_owners(neo4j, records)
            except Exception as exc:
                logger.error(
                    "Beneficial owner ingestion failed: %s", exc, exc_info=True
                )

    logger.info("Äriregister pipeline complete")


def main() -> None:
    """Synchronous entry point for CLI invocation."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
