from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.mides import MidesPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> MidesPipeline:
    return MidesPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestMidesMetadata:
    def test_name(self) -> None:
        assert MidesPipeline.name == "mides"

    def test_source_id(self) -> None:
        assert MidesPipeline.source_id == "mides"


class TestMidesTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.bids) == 2
        assert len(pipeline.contracts) == 2
        assert len(pipeline.items) == 2

    def test_links_companies(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        bid_cnpjs = {row["cnpj"] for row in pipeline.bid_company_rels}
        contract_cnpjs = {row["cnpj"] for row in pipeline.contract_company_rels}

        assert "11.222.333/0001-81" in bid_cnpjs
        assert "11.222.333/0001-81" in contract_cnpjs
        assert "22.333.444/0001-90" in contract_cnpjs

    def test_contract_item_rels(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.contract_item_rels) == 2


class TestMidesLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
