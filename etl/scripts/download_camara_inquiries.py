#!/usr/bin/env python3
"""Download Câmara CPI/CPMI metadata, requirements and sessions.

Outputs canonical CSV files consumed by CamaraInquiriesPipeline:
- data/camara_inquiries/inquiries.csv
- data/camara_inquiries/requirements.csv
- data/camara_inquiries/sessions.csv
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

import click
import httpx

logger = logging.getLogger(__name__)

CAMARA_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"


def _request_json(
    client: httpx.Client,
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = client.get(url, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {}


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dados = payload.get("dados")
    if isinstance(dados, list):
        return [x for x in dados if isinstance(x, dict)]
    return []


def _query_orgaos(client: httpx.Client, sigla: str) -> list[dict[str, Any]]:
    payload = _request_json(
        client,
        f"{CAMARA_BASE_URL}/orgaos",
        {"sigla": sigla, "itens": 100},
    )
    return _extract_items(payload)


def _kind_from_sigla(sigla: str) -> str:
    upper = sigla.upper()
    if "CPMI" in upper:
        return "CPMI"
    return "CPI"


def _safe_list(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return []
        current = current.get(key)
    if isinstance(current, list):
        return [x for x in current if isinstance(x, dict)]
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        logger.warning("No rows for %s", path.name)
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


@click.command()
@click.option("--output-dir", default="./data/camara_inquiries", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True)
def main(output_dir: str, skip_existing: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    inquiries_csv = out / "inquiries.csv"
    req_csv = out / "requirements.csv"
    sessions_csv = out / "sessions.csv"

    if skip_existing and inquiries_csv.exists() and req_csv.exists() and sessions_csv.exists():
        logger.info("Skipping download (all outputs exist).")
        return

    inquiries: list[dict[str, Any]] = []
    requirements: list[dict[str, Any]] = []
    sessions: list[dict[str, Any]] = []

    with httpx.Client(headers={"Accept": "application/json"}, follow_redirects=True) as client:
        orgaos = _query_orgaos(client, "CPI") + _query_orgaos(client, "CPMI")
        logger.info("Found %d candidate inquiry orgaos", len(orgaos))

        seen_orgaos: set[str] = set()
        for orgao in orgaos:
            orgao_id = str(orgao.get("id", "")).strip()
            if not orgao_id or orgao_id in seen_orgaos:
                continue
            seen_orgaos.add(orgao_id)

            sigla = str(orgao.get("sigla", "")).strip()
            nome = str(orgao.get("nomePublicacao") or orgao.get("nome", "")).strip()
            if "CPI" not in sigla.upper() and "CPI" not in nome.upper():
                continue

            inquiry_id = f"camara-{orgao_id}"
            inquiry_url = f"{CAMARA_BASE_URL}/orgaos/{orgao_id}"

            details = _request_json(client, inquiry_url)
            dado = details.get("dados") if isinstance(details.get("dados"), dict) else {}

            inquiries.append({
                "inquiry_id": inquiry_id,
                "inquiry_code": sigla,
                "name": nome,
                "kind": _kind_from_sigla(sigla or nome),
                "house": "camara",
                "status": str(dado.get("situacao") or "").strip(),
                "subject": str(dado.get("descricao") or "").strip(),
                "date_start": str(dado.get("dataInicio") or "").strip()[:10],
                "date_end": str(dado.get("dataFim") or "").strip()[:10],
                "source_url": inquiry_url,
            })

            eventos_payload = _request_json(
                client,
                f"{CAMARA_BASE_URL}/orgaos/{orgao_id}/eventos",
                {"itens": 200},
            )
            for event in _extract_items(eventos_payload):
                sessions.append({
                    "session_id": f"camara-event-{event.get('id', '')}",
                    "inquiry_id": inquiry_id,
                    "date": str(event.get("dataHoraInicio") or "").strip()[:10],
                    "topic": str(event.get("descricaoTipo") or event.get("titulo") or "").strip(),
                    "source_url": str(event.get("uri") or inquiry_url),
                })

            proposicoes_payload = _request_json(
                client,
                f"{CAMARA_BASE_URL}/orgaos/{orgao_id}/proposicoes",
                {"itens": 200},
            )
            for prop in _extract_items(proposicoes_payload):
                prop_id = str(prop.get("id", "")).strip()
                if not prop_id:
                    continue
                requirements.append({
                    "requirement_id": f"camara-req-{prop_id}",
                    "inquiry_id": inquiry_id,
                    "type": str(prop.get("siglaTipo") or "").strip(),
                    "date": str(prop.get("dataApresentacao") or "").strip()[:10],
                    "text": str(prop.get("ementa") or prop.get("descricaoTipo") or "").strip(),
                    "status": str(
                        prop.get("statusProposicao", {}).get("descricaoSituacao")
                        if isinstance(prop.get("statusProposicao"), dict)
                        else ""
                    ).strip(),
                    "author_name": str(prop.get("nomeAutor") or "").strip(),
                    "author_cpf": "",
                    "source_url": str(prop.get("uri") or inquiry_url),
                })

    _write_csv(inquiries_csv, inquiries)
    _write_csv(req_csv, requirements)
    _write_csv(sessions_csv, sessions)


if __name__ == "__main__":
    main()
