from bracc_etl.transforms import (
    classify_document,
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    strip_document,
    validate_cnpj,
    validate_cpf,
)


class TestNameNormalization:
    def test_basic(self) -> None:
        assert normalize_name("João da Silva") == "JOAO DA SILVA"

    def test_accents_removed(self) -> None:
        assert normalize_name("José André Müller") == "JOSE ANDRE MULLER"

    def test_extra_whitespace(self) -> None:
        assert normalize_name("  Maria   Helena  ") == "MARIA HELENA"

    def test_empty(self) -> None:
        assert normalize_name("") == ""
        assert normalize_name(None) == ""


class TestDocumentFormatting:
    def test_strip_document(self) -> None:
        assert strip_document("123.456.789-09") == "12345678909"
        assert strip_document("12.345.678/0001-99") == "12345678000199"
        assert strip_document(None) == ""

    def test_format_cpf(self) -> None:
        assert format_cpf("12345678909") == "123.456.789-09"
        assert format_cpf("123.456.789-09") == "123.456.789-09"

    def test_format_cpf_invalid_length(self) -> None:
        assert format_cpf("123") == "123"

    def test_format_cnpj(self) -> None:
        assert format_cnpj("12345678000199") == "12.345.678/0001-99"
        assert format_cnpj("12.345.678/0001-99") == "12.345.678/0001-99"

    def test_format_cnpj_invalid_length(self) -> None:
        assert format_cnpj("123") == "123"

    def test_validate_cpf_valid(self) -> None:
        # 529.982.247-25 is a valid CPF (check digit correct)
        assert validate_cpf("52998224725") is True

    def test_validate_cpf_invalid(self) -> None:
        assert validate_cpf("11111111111") is False
        assert validate_cpf("12345678900") is False
        assert validate_cpf("123") is False
        assert validate_cpf(None) is False

    def test_validate_cnpj_valid(self) -> None:
        # 11.222.333/0001-81 is a valid CNPJ
        assert validate_cnpj("11222333000181") is True

    def test_validate_cnpj_invalid(self) -> None:
        assert validate_cnpj("11111111111111") is False
        assert validate_cnpj("12345678000199") is False
        assert validate_cnpj("123") is False
        assert validate_cnpj(None) is False

    def test_classify_document(self) -> None:
        assert classify_document("12345678901") == "cpf_valid"
        assert classify_document("123.456.789-01") == "cpf_valid"
        assert classify_document("***123456**") == "cpf_partial"
        assert classify_document("***.123.456-**") == "cpf_partial"
        assert classify_document("12.345.678/0001-99") == "cnpj_valid"
        assert classify_document("123") == "invalid"


class TestDeduplication:
    def test_basic(self) -> None:
        rows = [
            {"cpf": "111", "name": "A"},
            {"cpf": "222", "name": "B"},
            {"cpf": "111", "name": "A-dup"},
        ]
        result = deduplicate_rows(rows, ["cpf"])
        assert len(result) == 2
        assert result[0]["name"] == "A"

    def test_composite_key(self) -> None:
        rows = [
            {"cpf": "111", "year": 2020},
            {"cpf": "111", "year": 2024},
            {"cpf": "111", "year": 2020},
        ]
        result = deduplicate_rows(rows, ["cpf", "year"])
        assert len(result) == 2

    def test_empty(self) -> None:
        assert deduplicate_rows([], ["cpf"]) == []
