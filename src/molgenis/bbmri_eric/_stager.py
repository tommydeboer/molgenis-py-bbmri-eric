from typing import List

from molgenis.bbmri_eric._model import ExternalServerNode, TableType
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.errors import EricWarning
from molgenis.bbmri_eric.printer import Printer


class Stager:
    def __init__(self, session: BbmriSession, printer: Printer = None):
        self.session = session
        self.printer: Printer = printer if printer else Printer()
        self.warnings: List[EricWarning] = []

    def stage(self, node: ExternalServerNode) -> List[EricWarning]:
        """
        Stages all data from the provided external nodes in the BBMRI-ERIC directory.
        """
        self.warnings = []

        self.printer.print(f"Clearing staging area of {node.code}")
        self._clear_staging_area(node)

        self.printer.print(
            f"Importing data from {node.url} to staging area of {node.code}"
        )
        self._import_node(node)

        return self.warnings

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
        self.printer.indent()
        source_session = BbmriSession(url=node.url)
        source_data = source_session.get_node_data(node, staging=False)

        for table in source_data.import_order:
            target_name = node.get_staging_id(table.type)

            self.printer.print(f"Importing data to {target_name}")
            self.session.add_batched(target_name, table.rows)
        self.printer.dedent()
