from typing import List, Optional

from molgenis.bbmri_eric.bbmri_client import AttributesRequest, EricSession
from molgenis.bbmri_eric.errors import EricError, ErrorReport, requests_error_handler
from molgenis.bbmri_eric.model import ExternalServerNode, Node, NodeData
from molgenis.bbmri_eric.pid_manager import PidManagerFactory
from molgenis.bbmri_eric.pid_service import BasePidService
from molgenis.bbmri_eric.printer import Printer
from molgenis.bbmri_eric.publisher import Publisher, PublishingState
from molgenis.bbmri_eric.stager import Stager
from molgenis.bbmri_eric.transformer import Transformer
from molgenis.bbmri_eric.validation import Validator


class Eric:
    """
    Main class for doing operations on the ERIC directory.
    """

    def __init__(
        self, session: EricSession, pid_service: Optional[BasePidService] = None
    ):
        """
        :param session: an authenticated session with an ERIC directory
        :param pid_service: a configured PidService, required for publishing. When no
        PidService is provided, nodes can only be staged.
        """
        self.session = session
        self.printer = Printer()
        self.pid_service: Optional[BasePidService] = pid_service
        if pid_service:
            self.pid_manager = PidManagerFactory.create(self.pid_service, self.printer)

    def stage_external_nodes(self, nodes: List[ExternalServerNode]) -> ErrorReport:
        """
        Stages all data from the provided external nodes in the ERIC directory.

        Parameters:
            nodes (List[ExternalServerNode]): The list of external nodes to stage
        """
        report = ErrorReport(nodes)
        for node in nodes:
            self.printer.print_node_title(node)
            try:
                self._stage_node(node)
            except EricError as e:
                self.printer.print_error(e)
                report.add_node_error(node, e)

        self.printer.print_summary(report)
        return report

    def publish_nodes(self, nodes: List[Node]) -> ErrorReport:
        """
        Publishes data from the provided nodes to the production tables in the ERIC
        directory.

        Parameters:
            nodes (List[Node]): The list of nodes to publish
        """
        if not self.pid_service:
            raise ValueError("A PID service is required to publish nodes")

        report = ErrorReport(nodes)
        try:
            state = self._init_state(nodes, report)
        except EricError as e:
            self.printer.print_error(e)
            report.set_global_error(e)
        else:
            self._prepare_nodes(nodes, state)
            self._publish_nodes(state)

        self.printer.print_summary(report)
        return report

    @requests_error_handler
    def _init_state(self, nodes: List[Node], report: ErrorReport) -> PublishingState:
        self.printer.print_header("⚙️ Preparation")

        self.printer.print("📦 Retrieving existing published data")
        published_data = self.session.get_published_data(
            nodes,
            AttributesRequest(
                persons=["id", "national_node"],
                networks=["id", "national_node"],
                biobanks=["id", "pid", "name", "national_node"],
                collections=["id", "national_node"],
            ),
        )

        self.printer.print("📦 Retrieving quality information")
        quality_info = self.session.get_quality_info()

        self.printer.print("📦 Retrieving data of node EU")
        eu_node_data = self.session.get_staging_node_data(self.session.get_node("EU"))

        return PublishingState(
            existing_data=published_data,
            quality_info=quality_info,
            eu_node_data=eu_node_data,
            nodes=nodes,
            report=report,
        )

    def _prepare_nodes(self, nodes, state):
        for node in nodes:
            self.printer.print_node_title(node)
            try:
                node_data = self._prepare_node_data(node, state)
                state.data_to_publish.merge(node_data)
            except EricError as e:
                self.printer.print_error(e)
                state.existing_data.remove_node_rows(node)
                state.report.add_node_error(node, e)

    def _publish_nodes(self, state: PublishingState):
        try:
            self.printer.print_header(
                f"🎁 Publishing node{'s' if len(state.nodes) > 1 else ''}"
            )
            Publisher(
                self.session, self.printer, state.quality_info, self.pid_manager
            ).publish(state)
        except EricError as e:
            self.printer.print_error(e)
            state.report.set_global_error(e)

    @requests_error_handler
    def _prepare_node_data(self, node: Node, state: PublishingState) -> NodeData:
        if isinstance(node, ExternalServerNode):
            self._stage_node(node)

        node_data = self._get_node_data(node)
        self._validate_node(node_data, state.report)
        self._transform_node(node_data, state)
        self._manage_node_pids(node_data, state)

        return node_data

    @requests_error_handler
    def _stage_node(self, node: ExternalServerNode):
        self.printer.print(f"📥 Staging data of node {node.code}")
        with self.printer.indentation():
            Stager(self.session, self.printer).stage(node)

    def _transform_node(self, node_data: NodeData, state: PublishingState):
        self.printer.print("✏️ Preparing staged data for publishing")
        with self.printer.indentation():
            warnings = Transformer(
                node_data=node_data,
                quality=state.quality_info,
                printer=self.printer,
                existing_biobanks=state.existing_data.biobanks,
                eu_node_data=state.eu_node_data,
            ).enrich()
            if warnings:
                state.report.add_node_warnings(node_data.node, warnings)

    def _manage_node_pids(self, node_data: NodeData, state: PublishingState):
        self.printer.print("🆔 Managing PIDs")
        with self.printer.indentation():
            warnings = self.pid_manager.assign_biobank_pids(node_data.biobanks)
            self.pid_manager.update_biobank_pids(
                node_data.biobanks, state.existing_data.biobanks
            )
            if warnings:
                state.report.add_node_warnings(node_data.node, warnings)

    def _validate_node(self, node_data: NodeData, report: ErrorReport):
        self.printer.print(f"🔎 Validating staged data of node {node_data.node.code}")
        with self.printer.indentation():
            warnings = Validator(node_data, self.printer).validate()
            if warnings:
                report.add_node_warnings(node_data.node, warnings)

    @requests_error_handler
    def _get_node_data(self, node: Node) -> NodeData:
        self.printer.print(f"📦 Retrieving staged data of node {node.code}")
        return self.session.get_staging_node_data(node)
