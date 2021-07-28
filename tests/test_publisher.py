from unittest import mock
from unittest.mock import MagicMock, patch

from molgenis.bbmri_eric._model import NodeData, TableType
from molgenis.bbmri_eric._printer import Printer
from molgenis.bbmri_eric._publisher import Publisher
from molgenis.bbmri_eric.bbmri_client import BbmriSession


@patch("molgenis.bbmri_eric._enricher.Enricher.enrich")
@patch("molgenis.bbmri_eric._publisher.Publisher._get_quality_info")
def test_publish(enrich_func, get_quality_info_func, node_data: NodeData):
    get_quality_info_func.return_value = {}
    session = BbmriSession("url")
    session.upsert_batched = MagicMock()
    publisher = Publisher(session, Printer())
    publisher._delete_rows = MagicMock()

    publisher.publish(node_data)

    enrich_func.assert_called()
    assert session.upsert_batched.mock_calls == [
        mock.call("eu_bbmri_eric_persons", node_data.persons.rows),
        mock.call("eu_bbmri_eric_networks", node_data.networks.rows),
        mock.call("eu_bbmri_eric_biobanks", node_data.biobanks.rows),
        mock.call("eu_bbmri_eric_collections", node_data.collections.rows),
    ]
    assert publisher._delete_rows.mock_calls == [
        mock.call(node_data.collections, node_data.node),
        mock.call(node_data.biobanks, node_data.node),
        mock.call(node_data.networks, node_data.node),
        mock.call(node_data.persons, node_data.node),
    ]


@patch("molgenis.bbmri_eric._publisher.Publisher._get_quality_info")
def test_delete_rows(get_quality_info_func, node_data):
    get_quality_info_func.return_value = {TableType.BIOBANKS: {"undeletable_id"}}
    session = BbmriSession("url")
    session.delete_list = MagicMock()
    session.get = MagicMock()
    session.get.return_value = [
        {"id": "bbmri-eric:ID:NO_OUS", "national_node": "NO"},
        {"id": "ignore_this_row", "national_node": "XX"},
        {"id": "delete_this_row", "national_node": "NO"},
        {"id": "undeletable_id", "national_node": "NO"},
    ]
    publisher = Publisher(session, Printer())

    publisher._delete_rows(node_data.biobanks, node_data.node)

    session.get.assert_called_with(
        "eu_bbmri_eric_biobanks", batch_size=10000, attributes="id,national_node"
    )
    session.delete_list.assert_called_with(
        "eu_bbmri_eric_biobanks", ["delete_this_row"]
    )
    publisher.warnings = [
        "Prevented the deletion of a row that is referenced from "
        "the quality info: biobanks undeletable_id."
    ]


def test_get_quality_info():
    session = BbmriSession("url")
    session.get = MagicMock()
    session.get.side_effect = [
        [{"biobank": {"id": "biobank1"}}, {"biobank": {"id": "biobank2"}}],
        [{"collection": {"id": "collection1"}}],
    ]

    publisher = Publisher(session, Printer())

    assert publisher.quality_info == {
        TableType.BIOBANKS: {"biobank1", "biobank2"},
        TableType.COLLECTIONS: {"collection1"},
    }
