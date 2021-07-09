from typing import List

from molgenis.bbmri_eric._model import ExternalNode, TableType
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.client import MolgenisRequestError


class Stager:
    def __init__(self, session: BbmriSession):
        self.session = session

    def stage(self, external_nodes: List[ExternalNode]):
        """
        Stages all data from the provided external nodes in the BBMRI-ERIC directory.
        """
        for node in external_nodes:
            self._clear_staging_area(node)

            try:
                self._stage_node(node)
                print("\n")
            except MolgenisRequestError as exception:  # rollback?
                # TODO print error and continue to next node
                raise exception

    def _clear_staging_area(self, node: ExternalNode):
        """
        Deletes all data in the staging area of an external node
        """
        print(f"Clearing staging area {node.code} on {self.session.url}")

        for table_type in reversed(TableType.get_import_order()):
            self.session.delete(node.get_staging_id(table_type))

    def _stage_node(self, node: ExternalNode):
        """
        Get data from staging area to their own entity on 'self'
        """
        print(f"Importing data for staging area {node.code} on {self.session.url}\n")

        source_session = BbmriSession(url=node.url)
        source_data = source_session.get_node_data(node, staging=False)

        for table in source_data.tables:
            target_name = node.get_staging_id(table.type)

            print("Importing data to", target_name)
            self.session.add_batched(target_name, table.rows)
