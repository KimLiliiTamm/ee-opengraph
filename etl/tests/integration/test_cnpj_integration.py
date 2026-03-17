from pathlib import Path

import pytest
from neo4j import Driver

from bracc_etl.pipelines.cnpj import CNPJPipeline

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.mark.integration
def test_cnpj_pipeline_loads_to_neo4j(neo4j_driver: Driver) -> None:
    """Load CNPJ fixture data into real Neo4j and verify node/relationship counts."""
    # The fixture CSVs are in etl/tests/fixtures/ — pipeline expects data_dir/cnpj/
    # So we point data_dir to fixtures parent with a "cnpj" symlink or use the parent dir.
    # Since the existing pipeline reads from data_dir/cnpj/empresas.csv, we need to
    # adjust: the fixtures use cnpj_empresas.csv, not cnpj/empresas.csv.
    # For integration test, we manually set up the pipeline data.
    import pandas as pd

    pipeline = CNPJPipeline(driver=neo4j_driver, data_dir=str(FIXTURES.parent))

    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False
    )

    pipeline.transform()
    pipeline.load()

    # Verify Company nodes
    with neo4j_driver.session() as session:
        result = session.run("MATCH (c:Company) RETURN count(c) AS cnt")
        record = result.single()
        assert record is not None
        assert record["cnt"] >= 3

    # Verify Person nodes
    with neo4j_driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN count(p) AS cnt")
        record = result.single()
        assert record is not None
        assert record["cnt"] >= 3

    # Verify SOCIO_DE relationships
    with neo4j_driver.session() as session:
        result = session.run("MATCH ()-[r:SOCIO_DE]->() RETURN count(r) AS cnt")
        record = result.single()
        assert record is not None
        assert record["cnt"] >= 3


@pytest.mark.integration
def test_cnpj_pipeline_deduplicates(neo4j_driver: Driver) -> None:
    """Running the pipeline twice should not create duplicate nodes."""
    import pandas as pd

    pipeline = CNPJPipeline(driver=neo4j_driver, data_dir=str(FIXTURES.parent))

    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False
    )

    pipeline.transform()
    pipeline.load()

    # Run again — should not duplicate
    pipeline.transform()
    pipeline.load()

    with neo4j_driver.session() as session:
        result = session.run("MATCH (c:Company) RETURN count(c) AS cnt")
        record = result.single()
        assert record is not None
        # Should still be 3 companies (MERGE prevents duplicates)
        assert record["cnt"] == 3
