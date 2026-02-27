#!/usr/bin/env python3
"""Download Senate inquiry data (CPI/CPMI) in v2 canonical format.

Output files consumed by SenadoCpisPipeline v2:
- data/senado_cpis/inquiries.csv
- data/senado_cpis/requirements.csv
- data/senado_cpis/sessions.csv
- data/senado_cpis/members.csv

Strategy:
1) Try Senado official open data endpoint(s).
2) Fallback to BigQuery cpipedia if official endpoint fails or returns no rows.
"""

from __future__ import annotations

import csv
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import click
import httpx

logger = logging.getLogger(__name__)

SENADO_OPEN_DATA = "https://legis.senado.leg.br/dadosabertos"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        logger.warning("No rows for %s", path.name)
        path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    logger.info("Wrote %d rows to %s", len(rows), path)


def _text(node: ET.Element | None) -> str:
    return (node.text if node is not None and node.text else "").strip()


def _fetch_official_inquiries(client: httpx.Client) -> list[dict[str, str]]:
    """Best-effort parser for Senado commissions endpoint.

    The response structure can vary across versions; parser is tolerant.
    """
    rows: list[dict[str, str]] = []

    for sigla in ("CPI", "CPMI"):
        url = f"{SENADO_OPEN_DATA}/comissao/lista"
        try:
            resp = client.get(url, params={"sigla": sigla}, timeout=60)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
        except Exception:  # noqa: BLE001
            logger.warning("Official Senado endpoint failed for sigla=%s", sigla)
            continue

        commissions = root.findall(".//Comissao")
        for com in commissions:
            code = _text(com.find("Codigo")) or _text(com.find("CodigoComissao"))
            name = _text(com.find("Nome")) or _text(com.find("NomeComissao"))
            if not name:
                continue

            rows.append({
                "inquiry_id": f"senado-{code or name[:20]}",
                "inquiry_code": code,
                "name": name,
                "kind": sigla,
                "house": "senado",
                "status": _text(com.find("Status")),
                "subject": _text(com.find("Descricao")),
                "date_start": _text(com.find("DataInicio"))[:10],
                "date_end": _text(com.find("DataFim"))[:10],
                "source_url": url,
            })

    return rows


def _fallback_from_bigquery(
    billing_project: str,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Fallback to cpipedia BigQuery to preserve historical coverage."""
    try:
        import google.auth
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError("Install optional deps: pip install '.[bigquery]'") from exc

    credentials, _ = google.auth.default()
    client = bigquery.Client(project=billing_project, credentials=credentials)

    query = """
    SELECT *
    FROM `basedosdados.br_senado_cpipedia.microdados`
    """

    result = client.query(query).result()

    inquiries: list[dict[str, str]] = []
    members: list[dict[str, str]] = []

    for row in result:
        data = dict(row)
        code = str(data.get("codigo_cpi", "") or "").strip()
        name = str(data.get("nome_cpi", "") or "").strip()
        if not name:
            continue

        inquiry_id = f"senado-{code or name[:20]}"
        kind = "CPMI" if "CPMI" in name.upper() else "CPI"

        inquiries.append({
            "inquiry_id": inquiry_id,
            "inquiry_code": code,
            "name": name,
            "kind": kind,
            "house": "senado",
            "status": "",
            "subject": str(data.get("objeto", "") or "").strip(),
            "date_start": str(data.get("data_inicio", "") or "").strip()[:10],
            "date_end": str(data.get("data_fim", "") or "").strip()[:10],
            "source_url": "https://basedosdados.org/dataset/br-senado-cpipedia",
        })

        member_name = str(data.get("nome_parlamentar", "") or "").strip()
        if member_name:
            members.append({
                "inquiry_id": inquiry_id,
                "member_name": member_name,
                "role": str(data.get("papel", "") or "").strip(),
            })

    return inquiries, members


@click.command()
@click.option("--billing-project", default="icarus-corruptos", help="GCP billing project")
@click.option("--output-dir", default="./data/senado_cpis", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True)
def main(billing_project: str, output_dir: str, skip_existing: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    inquiries_path = out / "inquiries.csv"
    req_path = out / "requirements.csv"
    sessions_path = out / "sessions.csv"
    members_path = out / "members.csv"

    outputs = (inquiries_path, req_path, sessions_path, members_path)
    if skip_existing and all(path.exists() for path in outputs):
        logger.info("Skipping (all outputs exist)")
        return

    inquiries: list[dict[str, str]] = []
    requirements: list[dict[str, str]] = []
    sessions: list[dict[str, str]] = []
    members: list[dict[str, str]] = []

    with httpx.Client(follow_redirects=True) as client:
        inquiries = _fetch_official_inquiries(client)

    if not inquiries:
        logger.warning("Official Senado API returned no rows; falling back to BigQuery cpipedia")
        try:
            inquiries, members = _fallback_from_bigquery(billing_project)
        except Exception as exc:  # noqa: BLE001
            logger.warning("BigQuery fallback failed: %s", exc)

    # Keep empty requirements/sessions file as explicit placeholders for v2 ingestion.
    _write_csv(inquiries_path, inquiries)
    _write_csv(req_path, requirements)
    _write_csv(sessions_path, sessions)
    _write_csv(members_path, members)


if __name__ == "__main__":
    main()
