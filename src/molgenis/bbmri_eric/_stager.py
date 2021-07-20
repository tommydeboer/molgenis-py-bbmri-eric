from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List

from molgenis.bbmri_eric._model import ExternalServerNode, Node, TableType
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.client import MolgenisRequestError


@dataclass
class EricError:
    message: str


@dataclass
class StagingReport:
    errors: DefaultDict[Node, EricError] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_error(self, node: Node, error: EricError):
        self.errors[node] = error

    def has_errors(self):
        return len(self.errors) > 0


class Stager:
    def __init__(self, session: BbmriSession):
        self.session = session
        self.report = StagingReport()

    def stage(self, external_nodes: List[ExternalServerNode]) -> StagingReport:
        """
        Stages all data from the provided external nodes in the BBMRI-ERIC directory.
        """
        for node in external_nodes:
            try:
                self._import_node(node)
            except MolgenisRequestError as e:
                error = EricError(f"Staging of node {node.code} failed: {e.message}")
                print(error.message)
                self.report.add_error(node, error)
        return self.report

    def _stage_node(self, node: ExternalServerNode):
        print(f"Clearing staging area of {node.code}")
        self._clear_staging_area(node)

        print(f"Importing data from {node.url} to staging area of {node.code}")
        self._import_node(node)

    def _clear_staging_area(self, node: ExternalServerNode):
        """
        Deletes all data in the staging area of an external node
        """
        for table_type in reversed(TableType.get_import_order()):
            self.session.delete(node.get_staging_id(table_type))

    def _import_node(self, node: ExternalServerNode):
        """
        Get data from staging area to their own entity on 'self'
        """
        source_session = BbmriSession(url=node.url)
        source_data = source_session.get_node_data(node, staging=False)

        for table in source_data.import_order:
            target_name = node.get_staging_id(table.type)

            print(f"  Importing data to {table.full_name}")
            self.session.add_batched(target_name, table.rows)
