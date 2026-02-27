from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from icarus_etl.pipelines.querido_diario import QueridoDiarioPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> QueridoDiarioPipeline:
    return QueridoDiarioPipeline(driver=MagicMock(), data_dir=str(FIXTURES))


class TestQueridoDiarioMetadata:
    def test_name(self) -> None:
        assert QueridoDiarioPipeline.name == "querido_diario"

    def test_source_id(self) -> None:
        assert QueridoDiarioPipeline.source_id == "querido_diario"


class TestQueridoDiarioTransform:
    def test_transform_counts(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        assert len(pipeline.acts) == 2
        assert len(pipeline.company_mentions) == 1

    def test_extracts_cnpj_mentions(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()

        mention = pipeline.company_mentions[0]
        assert mention["cnpj"] == "11.222.333/0001-81"
        assert mention["method"] == "text_cnpj_extract"
        assert "extract_span" in mention


class TestQueridoDiarioLoad:
    def test_load_no_raise(self) -> None:
        pipeline = _make_pipeline()
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
