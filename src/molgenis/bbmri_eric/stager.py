from typing import List

from molgenis.bbmri_eric import utils
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import ExternalNode, Node
from molgenis.client import MolgenisRequestError


class Stager:
    # TODO extract
    _IMPORT_SEQUENCE = ["persons", "networks", "biobanks", "collections"]
    _PACKAGE = "eu_bbmri_eric"

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

        for entity_name in reversed(self._IMPORT_SEQUENCE):
            target_entity = self._get_qualified_entity_name(
                entity_name=entity_name, node=node
            )
            target_data = self.session.get_all_rows(entity=target_entity)
            ids = utils.get_all_ids(target_data)
            previous_ids_per_entity[entity_name] = ids

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
        for entity_name in self._IMPORT_SEQUENCE:
            target_entity = self._get_qualified_entity_name(
                entity_name=entity_name, node=node
            )
            source_entity = self._get_qualified_entity_name(entity_name=entity_name)
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

    # TODO extract
    def _get_qualified_entity_name(self, entity_name: str, node: Node = None) -> str:
        """
        Method to create a correct name for an entity.
        """
        if node:
            return f"{self._PACKAGE}_{node.code}_{entity_name}"
        else:
            return f"{self._PACKAGE}_{entity_name}"
