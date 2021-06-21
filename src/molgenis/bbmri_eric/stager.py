from typing import List

from molgenis.bbmri_eric import model, utils
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import ExternalNode
from molgenis.client import MolgenisRequestError


class Stager:
    # TODO extract
    def __init__(self, session: BbmriSession):
        self.session = session

    def stage(self, external_nodes: List[ExternalNode]):
        """
        Stages all data from the provided external nodes in the BBMRI-ERIC directory.
        """
        for node in external_nodes:
            self._delete_national_node_own_entity_data(node)
            print("\n")

            try:
                self._import_national_node_to_own_entity(node)
                print("\n")
            except ValueError as exception:  # rollback?
                raise exception

    def _delete_national_node_own_entity_data(self, node: ExternalNode):
        """
        Delete data before import from national node entity
        """
        print(f"Deleting data for staging area {node.code} on {self.session._root_url}")

        previous_ids_per_entity = {}

        for table in reversed(model.get_import_sequence()):
            target_entity = table.get_staging_name(node)
            target_data = self.session.get_all_rows(entity=target_entity)
            ids = utils.get_all_ids(target_data)
            previous_ids_per_entity[table.name] = ids

            if len(ids) > 0:
                # delete from node specific
                print("Deleting data in", target_entity)
                try:
                    self.session.remove_rows(entity=target_entity, ids=ids)
                except ValueError as exception:
                    raise exception

        return previous_ids_per_entity

    def _import_national_node_to_own_entity(self, node: ExternalNode):
        """
        Get data from staging area to their own entity on 'self'
        """
        source_session = BbmriSession(url=node.url)

        print(
            f"Importing data for staging area {node.code} on {self.session._root_url}\n"
        )

        # imports
        for table in model.get_import_sequence():
            target_entity = table.get_staging_name(node)
            source_entity = table.get_fullname()
            source_data = source_session.get_all_rows(entity=source_entity)
            source_one_to_manys = source_session.get_one_to_manys(entity=source_entity)

            # import all the data
            if len(source_data) > 0:
                print("Importing data to", target_entity)
                prepped_source_data = utils.transform_to_molgenis_upload_format(
                    data=source_data, one_to_manys=source_one_to_manys
                )
                try:
                    self.session.bulk_add_all(
                        entity=target_entity, data=prepped_source_data
                    )
                except MolgenisRequestError as exception:
                    raise ValueError(exception)
