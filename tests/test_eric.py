from typing import List
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from molgenis.bbmri_eric.eric import Eric
from molgenis.bbmri_eric.errors import EricError, EricWarning, ErrorReport
from molgenis.bbmri_eric.model import ExternalServerNode, Node, NodeData, Source
from molgenis.bbmri_eric.publisher import PublishingState


@pytest.fixture
def report_init():
    with patch("molgenis.bbmri_eric.eric.ErrorReport") as report_mock:
        yield report_mock


@pytest.fixture
def stager_init():
    with patch("molgenis.bbmri_eric.eric.Stager") as stager_mock:
        yield stager_mock


@pytest.fixture
def validator_init():
    with patch("molgenis.bbmri_eric.eric.Validator") as validator_mock:
        yield validator_mock


@pytest.fixture
def publisher_init():
    with patch("molgenis.bbmri_eric.eric.Publisher") as publisher_mock:
        yield publisher_mock


@pytest.fixture
def transformer_init():
    with patch("molgenis.bbmri_eric.eric.Transformer") as transformer_mock:
        yield transformer_mock


@pytest.fixture
def pid_manager_factory():
    with patch(
        "molgenis.bbmri_eric.eric.PidManagerFactory"
    ) as pid_manager_factory_mock:
        yield pid_manager_factory_mock


def test_stage_external_nodes(stager_init, eric, printer):
    error = EricError("error")
    stager_init.return_value.stage.side_effect = [None, error]
    nl = ExternalServerNode("NL", "will succeed", "url.nl")
    be = ExternalServerNode("BE", "wil fail", "url.be")

    report = eric.stage_external_nodes([nl, be])

    assert printer.print_node_title.mock_calls == [mock.call(nl), mock.call(be)]
    assert stager_init.mock_calls == [
        mock.call(eric.session, eric.printer),
        mock.call().stage(nl),
        mock.call(eric.session, eric.printer),
        mock.call().stage(be),
    ]
    assert nl not in report.node_errors
    assert report.node_errors[be] == error
    printer.print_summary.assert_called_once_with(report)


def test_publish_node_staging_fails(
    eric,
    session,
    report_init,
    stager_init,
    validator_init,
    publisher_init,
):
    nl = ExternalServerNode("NL", "Netherlands", "url")
    state = _setup_state([nl], eric, report_init)

    error = EricError("error")
    stager_init.return_value.stage.side_effect = error

    report = eric.publish_nodes([nl])

    eric.printer.print_node_title.assert_called_once_with(nl)
    stager_init.assert_called_with(session, eric.printer)
    stager_init.return_value.stage.assert_called_with(nl)
    assert not session.get_published_node_data.called
    assert not validator_init.called
    publisher_init.assert_called_with(
        session, eric.printer, state.quality_info, eric.pid_manager
    )
    assert publisher_init.return_value.publish.called_with(state)
    assert report.node_errors[nl] == error
    eric.printer.print_summary.assert_called_once_with(report)


# noinspection PyProtectedMember
def _setup_state(nodes: List[Node], eric: Eric, report_init):
    report = ErrorReport(nodes)
    report_init.return_value = report

    state = PublishingState(
        existing_data=MagicMock(),
        eu_node_data=MagicMock(),
        quality_info=MagicMock(),
        nodes=nodes,
        report=report,
    )
    eric._init_state = MagicMock()
    eric._init_state.return_value = state
    return state


def test_publish_node_get_data_fails(
    eric,
    report_init,
    publisher_init,
    validator_init,
    stager_init,
    session,
):
    nl = ExternalServerNode("NL", "Netherlands", "url")
    state = _setup_state([nl], eric, report_init)

    error = EricError("error")
    session.get_staging_node_data.side_effect = error

    report = eric.publish_nodes([nl])

    eric.printer.print_node_title.assert_called_once_with(nl)
    publisher_init.assert_called_with(
        session, eric.printer, state.quality_info, eric.pid_manager
    )
    stager_init.assert_called_with(session, eric.printer)
    stager_init.return_value.stage.assert_called_with(nl)
    session.get_staging_node_data.assert_called_with(nl)
    assert not validator_init.called
    assert publisher_init.return_value.publish.called_with(state)
    assert report.node_errors[nl] == error
    eric.printer.print_summary.assert_called_once_with(report)


def test_publish_nodes(
    eric,
    pid_service,
    pid_manager_factory,
    report_init,
    publisher_init,
    validator_init,
    stager_init,
    transformer_init,
    session,
):
    no = Node("NO", "succeeds with validation warnings")
    nl = ExternalServerNode("NL", "fails during publishing", "url")
    state = _setup_state([no, nl], eric, report_init)

    pid_manager = MagicMock()
    pid_manager_factory.create.return_value = pid_manager
    pid_manager.assign_biobank_pids.return_value = []
    no_data = _mock_node_data(no)
    nl_data = _mock_node_data(nl)
    session.get_staging_node_data.side_effect = [no_data, nl_data]
    warning = EricWarning("warning")
    validator_init.return_value.validate.side_effect = [[warning], []]
    transformer_init.return_value.transform.return_value = []

    report = eric.publish_nodes([no, nl])

    assert eric.printer.print_node_title.mock_calls == [mock.call(no), mock.call(nl)]
    stager_init.assert_called_with(session, eric.printer)
    stager_init.return_value.stage.assert_called_with(nl)
    assert validator_init.mock_calls == [
        mock.call(no_data, eric.printer),
        mock.call().validate(),
        mock.call(nl_data, eric.printer),
        mock.call().validate(),
    ]
    assert publisher_init.mock_calls == [
        mock.call(session, eric.printer, state.quality_info, eric.pid_manager),
        mock.call().publish(state),
    ]
    assert no not in report.node_errors
    assert nl not in report.node_errors
    assert nl not in report.node_warnings
    assert report.node_warnings[no] == [warning]
    eric.printer.print_summary.assert_called_once_with(report)


def _mock_node_data(node: Node):
    return NodeData(
        node=node,
        source=Source.STAGING,
        persons=MagicMock(),
        biobanks=MagicMock(),
        networks=MagicMock(),
        collections=MagicMock(),
    )
