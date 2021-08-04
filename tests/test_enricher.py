from unittest.mock import MagicMock

from molgenis.bbmri_eric._enricher import Enricher
from molgenis.bbmri_eric._model import QualityInfo
from molgenis.bbmri_eric._printer import Printer


def test_enricher_node_codes(node_data):
    for table in node_data.import_order:
        assert "national_node" not in table.rows[0]

    Enricher(node_data, MagicMock(), Printer())._set_national_node_code()

    for table in node_data.import_order:
        assert table.rows[0]["national_node"] == "NO"


def test_enricher_commercial_use():
    # TODO
    pass


def test_enricher_quality(node_data):
    q_info = QualityInfo(
        biobanks={
            "bbmri-eric:ID:NO_BIOBANK1": "quality1",
            "bbmri-eric:ID:NO_CoronaTrondelag": "quality2",
        },
        collections={
            "bbmri-eric:ID:NO_bbmri-eric:ID:NO_CancerBiobankOUH:collection"
            ":all_samples_samples": "quality1"
        },
    )

    Enricher(node_data, q_info, Printer())._set_quality_info()

    assert node_data.biobanks.rows[0]["quality"] == "quality1"
    assert "quality" not in node_data.biobanks.rows[1]
    assert node_data.biobanks.rows[2]["quality"] == "quality2"
    assert node_data.collections.rows[0]["quality"] == "quality1"
    assert "quality" not in node_data.collections.rows[1]
