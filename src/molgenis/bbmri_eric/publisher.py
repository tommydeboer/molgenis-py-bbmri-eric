from typing import List

from molgenis.bbmri_eric import utils, validation
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import Node


class Publisher:
    # TODO extract
    _IMPORT_SEQUENCE = ["persons", "networks", "biobanks", "collections"]
    _PACKAGE = "eu_bbmri_eric"

    _TABLES_TO_CACHE = [
        "eu_bbmri_eric_bio_qual_info",
        "eu_bbmri_eric_col_qual_info",
    ]

    def __init__(self, session: BbmriSession):
        self.session = session
        self._combined_entity_cache = {}

    def publish(self, national_nodes: List[Node]):
        """
        Publishes data from the provided nodes to the production tables.
        """
        self._combined_entity_cache = {}

        self._prepare_deletion_of_node_data()

        try:

            for entity_name in reversed(self._IMPORT_SEQUENCE):
                for node in national_nodes:
                    self._delete_national_node_data_from_eric_entity(
                        node=node, entity_name=entity_name
                    )

            for import_entity_name in self._IMPORT_SEQUENCE:
                print("\n")
                for node in national_nodes:
                    self.__import_national_node_to_eric_entity(
                        node=node,
                        entity_name=import_entity_name,
                    )
                    print("\n")
        finally:
            self._replace_global_entities()

    def _prepare_deletion_of_node_data(self):
        """
        Checks the cache and makes one if not found
        """
        # verify we have it cached, if not start caching
        if not all(
            entity_name in self._combined_entity_cache
            for entity_name in self._IMPORT_SEQUENCE
        ):
            self._cache_combined_entity_data()

        for global_entity in self._TABLES_TO_CACHE:
            source_data = self._combined_entity_cache[global_entity]
            source_ids = utils.get_all_ids(source_data)
            self.session.remove_rows(entity=global_entity, ids=source_ids)

    def _cache_combined_entity_data(self) -> None:
        """
        Caches data for all bbmri entities, in case of rollback
        """
        for entity in self._IMPORT_SEQUENCE:
            source_entity = self._get_qualified_entity_name(entity_name=entity)
            source_data = self.session.get_all_rows(source_entity)
            source_one_to_manys = self.session.get_one_to_manys(source_entity)
            uploadable_source = utils.transform_to_molgenis_upload_format(
                data=source_data, one_to_manys=source_one_to_manys
            )

            self._combined_entity_cache[entity] = uploadable_source

        for global_entity in self._TABLES_TO_CACHE:
            source_data = self.session.get_all_rows(entity=global_entity)
            source_one_to_manys = self.session.get_one_to_manys(entity=global_entity)
            uploadable_source = utils.transform_to_molgenis_upload_format(
                data=source_data, one_to_manys=source_one_to_manys
            )

            self._combined_entity_cache[global_entity] = uploadable_source

    def _delete_national_node_data_from_eric_entity(self, node: Node, entity_name):
        """
        Surgically delete all national node data from combined entities
        """
        # sanity check
        if entity_name not in self._combined_entity_cache:
            self._cache_combined_entity_data()

        print(f"\nRemoving data from the entity: {entity_name} for: " f"{node.code}")
        entity_cached_data = self._combined_entity_cache[entity_name]
        target_entity = self._get_qualified_entity_name(entity_name=entity_name)
        national_node_data_for_entity = utils.filter_national_node_data(
            data=entity_cached_data, node=node
        )
        ids_for_national_node_data = utils.get_all_ids(national_node_data_for_entity)

        if len(ids_for_national_node_data) > 0:
            try:
                self.session.remove_rows(
                    entity=target_entity,
                    ids=ids_for_national_node_data,
                )
                print("Removed:", len(ids_for_national_node_data), "rows")
            except ValueError as exception:
                raise exception
        else:
            print("Nothing to remove for ", target_entity)
            print()

    def __import_national_node_to_eric_entity(self, node: Node, entity_name):
        """
        Import all national node data into the combined eric entities
        """

        print(f"Importing data for {node.code} on {self.session._root_url}\n")

        source = self._get_data_for_entity(entity_name=entity_name, node=node)
        target = self._get_data_for_entity(entity_name)

        valid_ids = [
            source_id
            for source_id in source["ids"]
            if validation.validate_bbmri_id(
                entity=entity_name, node=node, bbmri_id=source_id
            )
        ]

        # check for target ids because there could be eric
        # leftovers from the national node in the table.
        valid_entries = [
            valid_data
            for valid_data in source["data"]
            if valid_data["id"] in valid_ids and valid_data["id"] not in target["ids"]
        ]

        # check the ids per entity if they exist
        # > molgenis_utilities.get_all_ref_ids_by_entity

        if len(valid_entries) > 0:

            source_references = self.session.get_all_references_for_entity(
                entity=source["name"]
            )

            print("Importing data to", target["name"])
            prepped_source_data = utils.transform_to_molgenis_upload_format(
                data=valid_entries, one_to_manys=source_references["one_to_many"]
            )

            try:
                self.session.bulk_add_all(
                    entity=target["name"], data=prepped_source_data
                )
                print(
                    f"Imported: {len(prepped_source_data)} rows to {target['name']}"
                    f"out of {len(source['ids'])}"
                )
            except ValueError as exception:  # rollback
                print("\n")
                print("---" * 10)
                print("Failed to import, following error occurred:", exception)
                print("---" * 10)

                cached_data = self._combined_entity_cache[entity_name]
                original_data = utils.filter_national_node_data(
                    data=cached_data, node=node
                )
                ids_to_revert = utils.get_all_ids(data=prepped_source_data)

                if len(ids_to_revert) > 0:
                    self.session.remove_rows(entity=target["name"], ids=ids_to_revert)

                if len(original_data) > 0:
                    self.session.bulk_add_all(entity=target["name"], data=original_data)
                    print(
                        f"Rolled back {target['name']} with previous data for "
                        f"{node.code}"
                    )

    def _get_data_for_entity(self, entity_name: str, node: Node = None) -> dict:
        """
        Get's data for entity, if national node code is provided, it will fetch data
        from it's own entity
        """
        entity_name = self._get_qualified_entity_name(
            entity_name=entity_name, node=node
        )

        entity_data = self.session.get_all_rows(entity=entity_name)

        entity_ids = utils.get_all_ids(entity_data)

        return {"data": entity_data, "name": entity_name, "ids": entity_ids}

    def _replace_global_entities(self):
        """
        Function to loop over global entities and place back the data
        """
        print("Placing back the global entities")
        for global_entity in self._TABLES_TO_CACHE:
            source_data = self._combined_entity_cache[global_entity]
            if len(source_data) > 0:
                self.session.bulk_add_all(entity=global_entity, data=source_data)
                print(f"Placed back: {len(source_data)} rows to {global_entity}")
            else:
                print("No rows found to place back")

    # TODO extract
    def _get_qualified_entity_name(self, entity_name: str, node: Node = None) -> str:
        """
        Method to create a correct name for an entity.
        """
        if node:
            return f"{self._PACKAGE}_{node.code}_{entity_name}"
        else:
            return f"{self._PACKAGE}_{entity_name}"
