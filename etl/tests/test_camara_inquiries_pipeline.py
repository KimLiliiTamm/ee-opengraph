from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from bracc_etl.pipelines.camara_inquiries import CamaraInquiriesPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CamaraInquiriesPipeline:
    return CamaraInquiriesPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestCamaraInquiriesMetadata:
    def test_name(self) -> None:
        assert CamaraInquiriesPipeline.name == "camara_inquiries"

    def test_source_id(self) -> None:
        assert CamaraInquiriesPipeline.source_id == "camara_inquiries"


class TestCamaraInquiriesTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.inquiries) == 2
        assert len(pipeline.requirements) == 2
        assert len(pipeline.sessions) == 1

    def test_extracts_company_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        cnpjs = {m["cnpj"] for m in pipeline.requirement_company_mentions}
        assert "11.222.333/0001-81" in cnpjs
        assert "22.333.444/0001-90" in cnpjs

    def test_author_link_rows(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.requirement_author_cpf_rels) == 1
        assert len(pipeline.requirement_author_name_rels) == 1


class TestCamaraInquiriesLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
