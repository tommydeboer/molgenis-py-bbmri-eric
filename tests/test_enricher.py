from molgenis.bbmri_eric._enricher import Enricher
from molgenis.bbmri_eric._printer import Printer


def test_enricher_node_codes(node_data):
    for table in node_data.import_order:
        assert "national_node" not in table.rows[0]

    Enricher(node_data, Printer())._set_national_node_code()

    for table in node_data.import_order:
        assert table.rows[0]["national_node"] == "NO"


def test_enricher_commercial_use():
    # TODO
    pass
