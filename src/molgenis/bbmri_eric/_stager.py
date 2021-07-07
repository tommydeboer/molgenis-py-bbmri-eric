from typing import List

from molgenis.bbmri_eric import _utils
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import ExternalNode
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
            except ValueError as exception:  # rollback?
                raise exception

    def _clear_staging_area(self, node: ExternalNode):
        """
        Deletes all data in the staging area of an external node
        """
        print(f"Clearing staging area {node.code} on {self.session.url}")

        for table_name in reversed(node.get_staging_table_ids()):
            self.session.delete(table_name)

    def _stage_node(self, node: ExternalNode):
        """
        Get data from staging area to their own entity on 'self'
        """
        source_session = BbmriSession(url=node.url)

        print(f"Importing data for staging area {node.code} on {self.session.url}\n")

        # imports
        for target_name, source_name in zip(
            node.get_staging_table_ids(), node.get_external_table_ids()
        ):
            # TODO use session.get_node_data()
            source_data = source_session.get_all_rows(entity=source_name)
            source_ref_names = source_session.get_reference_attribute_names(source_name)

            # import all the data
            if len(source_data) > 0:
                print("Importing data to", target_name)
                prepped_source_data = _utils.transform_to_molgenis_upload_format(
                    data=source_data, one_to_manys=source_ref_names.one_to_manys
                )
                try:
                    self.session.bulk_add_all(
                        entity=target_name, data=prepped_source_data
                    )
                except MolgenisRequestError as exception:
                    raise ValueError(exception)
