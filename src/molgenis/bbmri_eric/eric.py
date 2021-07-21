from typing import List

from molgenis.bbmri_eric import _validation
from molgenis.bbmri_eric._model import ExternalServerNode, Node, NodeData
from molgenis.bbmri_eric._publisher import Publisher
from molgenis.bbmri_eric._stager import Stager
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.errors import EricError, ErrorReport
from molgenis.bbmri_eric.printer import Printer
from molgenis.client import MolgenisRequestError


class Eric:
    """
    Main class for doing operations on the ERIC directory.

    Attributes:
        session (BbmriSession): The session with an ERIC directory
    """

    def __init__(self, session: BbmriSession):
        """
        Parameters:
            session: an (authenticated) session with an ERIC directory
        """
        self.session = session
        self.printer = Printer()

    def stage_external_nodes(self, nodes: List[ExternalServerNode]) -> ErrorReport:
        """
        Stages all data from the provided external nodes in the ERIC directory.

        Parameters:
            nodes (List[ExternalServerNode]): The list of external nodes to stage
        """
        report = ErrorReport()
        stager = Stager(self.session)
        for node in nodes:
            self.printer.reset()
            self.printer.print_node_title(node)

            try:
                stager.stage(node)
            except EricError as e:
                self.printer.error(e)
                report.add_error(node, e)
                continue
        return report

    def publish_nodes(self, nodes: List[Node]) -> ErrorReport:
        """
        Publishes data from the provided nodes to the production tables in the ERIC
        directory.

        Parameters:
            nodes (List[Node]): The list of nodes to publish
        """
        report = ErrorReport()
        publisher = Publisher(self.session, self.printer)
        for node in nodes:
            self._publish_node(node, report, publisher)
        return report

    def _publish_node(self, node: Node, report: ErrorReport, publisher: Publisher):
        try:
            self.printer.reset()
            self.printer.print_node_title(node)

            if isinstance(node, ExternalServerNode):
                Stager(self.session).stage(node)

            node_data = self._get_node_data(node)
            self._validate_node(node_data, report)

            warnings = publisher.publish(node_data)
            report.add_warnings(node, warnings)

        except EricError as e:
            self.printer.error(e)
            report.add_error(node, e)

    def _validate_node(self, node_data: NodeData, report: ErrorReport):
        self.printer.print(f"🔎 Validating staging data of node {node_data.node.code}")
        warnings = _validation.validate_node(node_data)
        if warnings:
            report.add_warnings(node_data.node, warnings)
            self.printer.print_warnings(warnings)

    def _get_node_data(self, node: Node) -> NodeData:
        try:
            self.printer.print(f"📦 Retrieving staging data of node {node.code}")
            return self.session.get_node_data(node, staging=True)
        except MolgenisRequestError as e:
            raise EricError(f"Error retrieving data of node {node.code}") from e
