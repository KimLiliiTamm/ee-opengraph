from bracc_etl.entity_resolution.confidence import classify_confidence, normalize_score
from bracc_etl.entity_resolution.config import get_person_settings


class TestNormalizeScore:
    def test_clamp_above(self) -> None:
        assert normalize_score(1.5) == 1.0

    def test_clamp_below(self) -> None:
        assert normalize_score(-0.3) == 0.0

    def test_passthrough(self) -> None:
        assert normalize_score(0.85) == 0.85

    def test_boundaries(self) -> None:
        assert normalize_score(0.0) == 0.0
        assert normalize_score(1.0) == 1.0


class TestClassifyConfidence:
    def test_high(self) -> None:
        assert classify_confidence(0.95) == "high"
        assert classify_confidence(0.9) == "high"

    def test_medium(self) -> None:
        assert classify_confidence(0.85) == "medium"
        assert classify_confidence(0.7) == "medium"

    def test_low(self) -> None:
        assert classify_confidence(0.69) == "low"
        assert classify_confidence(0.0) == "low"


class TestGetPersonSettings:
    def test_returns_dict(self) -> None:
        try:
            settings = get_person_settings()
        except ImportError:
            # splink not installed — skip
            return
        assert isinstance(settings, dict)

    def test_has_comparisons(self) -> None:
        try:
            settings = get_person_settings()
        except ImportError:
            return
        assert "comparisons" in settings

    def test_has_blocking_rules(self) -> None:
        try:
            settings = get_person_settings()
        except ImportError:
            return
        assert "blocking_rules_to_generate_predictions" in settings
