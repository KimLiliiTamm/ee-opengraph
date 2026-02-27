#!/usr/bin/env python3
"""Download MiDES municipal procurement datasets from BigQuery.

Outputs canonical files consumed by MidesPipeline:
- data/mides/licitacao.csv
- data/mides/contrato.csv
- data/mides/item.csv
"""

from __future__ import annotations

import logging
from pathlib import Path

import click

logger = logging.getLogger(__name__)


def _run_query_to_csv(
    billing_project: str,
    query: str,
    output_path: Path,
    *,
    skip_existing: bool,
) -> None:
    if skip_existing and output_path.exists() and output_path.stat().st_size > 0:
        logger.info("Skipping (exists): %s", output_path)
        return

    try:
        import google.auth
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError("Install optional deps: pip install '.[bigquery]'") from exc

    credentials, _ = google.auth.default()
    client = bigquery.Client(project=billing_project, credentials=credentials)

    logger.info("Querying BigQuery into %s", output_path.name)
    df = client.query(query).result().to_dataframe(create_bqstorage_client=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Wrote %d rows to %s", len(df), output_path)


@click.command()
@click.option("--billing-project", default="icarus-corruptos", help="GCP billing project")
@click.option("--dataset", default="basedosdados.br_mides", help="BigQuery dataset id")
@click.option("--output-dir", default="./data/mides", help="Output directory")
@click.option("--start-year", type=int, default=2021, help="Filter start year")
@click.option("--end-year", type=int, default=2100, help="Filter end year")
@click.option("--skip-existing/--no-skip-existing", default=True)
def main(
    billing_project: str,
    dataset: str,
    output_dir: str,
    start_year: int,
    end_year: int,
    skip_existing: bool,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    year_filter = (
        f"WHERE SAFE_CAST(ano AS INT64) BETWEEN {start_year} AND {end_year}"
    )

    queries = {
        "licitacao.csv": (
            f"SELECT * FROM `{dataset}.licitacao` {year_filter}"
        ),
        "contrato.csv": (
            f"SELECT * FROM `{dataset}.contrato` {year_filter}"
        ),
        "item.csv": (
            f"SELECT * FROM `{dataset}.item` {year_filter}"
        ),
    }

    for filename, query in queries.items():
        try:
            _run_query_to_csv(
                billing_project,
                query,
                out / filename,
                skip_existing=skip_existing,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed %s: %s", filename, exc)


if __name__ == "__main__":
    main()
