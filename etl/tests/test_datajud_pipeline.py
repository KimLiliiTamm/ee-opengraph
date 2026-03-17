from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.datajud import DatajudPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> DatajudPipeline:
    return DatajudPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestDatajudMetadata:
    def test_name(self) -> None:
        assert DatajudPipeline.name == "datajud"

    def test_source_id(self) -> None:
        assert DatajudPipeline.source_id == "datajud"


class TestDatajudTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.cases) == 2
        assert len(pipeline.persons) == 1
        assert len(pipeline.companies) == 2

    def test_party_relationships(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.person_case_rels) == 1
        assert len(pipeline.company_case_rels) == 2


class TestDatajudLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
