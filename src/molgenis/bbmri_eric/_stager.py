from typing import List

from molgenis.bbmri_eric import _utils
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
            except ValueError as exception:  # rollback?
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
        source_session = BbmriSession(url=node.url)

        print(f"Importing data for staging area {node.code} on {self.session.url}\n")

        # imports
        for table_type in TableType.get_import_order():
            source_name = table_type.base_id
            target_name = node.get_staging_id(table_type)

            # TODO use session.get_node_data()
            source_data = source_session.get(entity=source_name, batch_size=10000)
            source_ref_names = source_session.get_reference_attribute_names(source_name)

            # import all the data
            if len(source_data) > 0:
                print("Importing data to", target_name)
                prepped_source_data = _utils.transform_to_molgenis_upload_format(
                    rows=source_data, one_to_manys=source_ref_names.one_to_manys
                )
                try:
                    self.session.add_batched(
                        entity_type_id=target_name, entities=prepped_source_data
                    )
                except MolgenisRequestError as exception:
                    raise ValueError(exception)
