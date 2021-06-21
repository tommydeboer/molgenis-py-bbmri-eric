"""
BBMRI interface for Molgenis
"""
from typing import List, Optional

import molgenis.bbmri_eric.bbmri_validations as bbmri_validations
import molgenis.bbmri_eric.molgenis_utilities as molgenis_utilities
from molgenis.bbmri_eric.nodes import ExternalNode, Node
from molgenis.client import MolgenisRequestError, Session

# Utility methods, maybe move elsewhere?


def _filter_national_node_data(data: List[dict], node: Node) -> List[dict]:
    """
    Filters data from an entity based on national node code in an Id
    """
    national_node_signature = f":{node.code}_"
    data_from_national_node = [
        row for row in data if national_node_signature in row["id"]
    ]
    return data_from_national_node


class BbmriSession(Session):
    """
    BBMRI Session Class, which extends the molgenis py client Session class
    """

    __PACKAGE = "eu_bbmri_eric_"
    __IMPORT_SEQUENCE = ["persons", "networks", "biobanks", "collections"]
    __TABLES_TO_CACHE = [
        "eu_bbmri_eric_bio_qual_info",
        "eu_bbmri_eric_col_qual_info",
    ]

    def __init__(self, url: str, token: Optional[str] = None):
        super().__init__(url, token)
        self.__combined_entity_cache = {}

    def stage(self, external_nodes: List[ExternalNode]):
        """
        Stages all data from the provided external nodes in the BBMRI-ERIC directory.
        """
        for node in external_nodes:
            self.__delete_national_node_own_entity_data(node)
            print("\n")

            try:
                self.__import_national_node_to_own_entity(node)
                print("\n")
            except ValueError as exception:  # rollback?
                raise exception

    def publish(self, national_nodes: List[Node]):
        """
        Publishes data from the provided nodes to the production tables.
        """
        self.__prepare_deletion_of_node_data()

        try:

            for entity_name in reversed(self.__IMPORT_SEQUENCE):
                for node in national_nodes:
                    self.__delete_national_node_data_from_eric_entity(
                        node=node, entity_name=entity_name
                    )

            for import_entity_name in self.__IMPORT_SEQUENCE:
                print("\n")
                for node in national_nodes:
                    self.__import_national_node_to_eric_entity(
                        node=node,
                        entity_name=import_entity_name,
                    )
                    print("\n")
        finally:
            self.__replace_global_entities()

    def __get_qualified_entity_name(self, entity_name: str, node: Node = None) -> str:
        """
        Method to create a correct name for an entity.
        """
        if node:
            return f"{self.__PACKAGE}{node.code}_{entity_name}"
        else:
            return f"{self.__PACKAGE}{entity_name}"

    def __get_data_for_entity(self, entity_name: str, node: Node = None) -> dict:
        """
        Get's data for entity, if national node code is provided, it will fetch data
        from it's own entity
        """
        entity_name = self.__get_qualified_entity_name(
            entity_name=entity_name, node=node
        )

        entity_data = molgenis_utilities.get_all_rows(session=self, entity=entity_name)

        entity_ids = molgenis_utilities.get_all_ids(entity_data)

        return {"data": entity_data, "name": entity_name, "ids": entity_ids}

    def __validate_refs(
        self,
        entity: str,
        entries: List[dict],
        node: Node,
    ) -> List[dict]:
        """
        Checks if any id in an xref or mref is invalid, if so then it omits that row
        """
        references = molgenis_utilities.get_all_references_for_entity(
            session=self, entity=entity
        )
        all_references = references["xref"]
        all_references.extend(references["one_to_many"])

        valid_entries = []

        for entry in entries:
            validations = bbmri_validations.validate_refs_in_entry(
                node=node,
                entry=entry,
                parent_entity=entity,
                possible_entity_references=all_references,
            )

            # check if the refs have been imported
            references_imported = []
            for validation in validations:
                eric_entity = self.__get_qualified_entity_name(
                    validation["entity_reference"]
                )
                try:
                    entry = self.get_by_id(entity=eric_entity, id_=validation["ref_id"])
                    references_imported.append(entry)
                except MolgenisRequestError:
                    break

            # only if all the references from this row are imported
            if len(references_imported) == len(validations):
                valid_entries.append(entry)

        return valid_entries

    def __cache_combined_entity_data(self) -> None:
        """
        Caches data for all bbmri entities, in case of rollback
        """
        for entity in self.__IMPORT_SEQUENCE:
            source_entity = self.__get_qualified_entity_name(entity_name=entity)
            source_data = molgenis_utilities.get_all_rows(
                session=self, entity=source_entity
            )
            source_one_to_manys = molgenis_utilities.get_one_to_manys(
                session=self, entity=source_entity
            )
            uploadable_source = molgenis_utilities.transform_to_molgenis_upload_format(
                data=source_data, one_to_manys=source_one_to_manys
            )

            self.__combined_entity_cache[entity] = uploadable_source

        for global_entity in self.__TABLES_TO_CACHE:
            source_data = molgenis_utilities.get_all_rows(
                session=self, entity=global_entity
            )
            source_one_to_manys = molgenis_utilities.get_one_to_manys(
                session=self, entity=global_entity
            )
            uploadable_source = molgenis_utilities.transform_to_molgenis_upload_format(
                data=source_data, one_to_manys=source_one_to_manys
            )

            self.__combined_entity_cache[global_entity] = uploadable_source

    def __import_national_node_to_own_entity(self, node: ExternalNode):
        """
        Get data from staging area to their own entity on 'self'
        """
        source_session = Session(url=node.url)

        print(f"Importing data for staging area {node.code} on {self._root_url}\n")

        # imports
        for entity_name in self.__IMPORT_SEQUENCE:
            target_entity = self.__get_qualified_entity_name(
                entity_name=entity_name, node=node
            )
            source_entity = self.__get_qualified_entity_name(entity_name=entity_name)
            source_data = molgenis_utilities.get_all_rows(
                session=source_session, entity=source_entity
            )
            source_one_to_manys = molgenis_utilities.get_one_to_manys(
                session=source_session, entity=source_entity
            )

            # import all the data
            if len(source_data) > 0:
                print("Importing data to", target_entity)
                prepped_source_data = (
                    molgenis_utilities.transform_to_molgenis_upload_format(
                        data=source_data, one_to_manys=source_one_to_manys
                    )
                )
                try:
                    molgenis_utilities.bulk_add_all(
                        session=self, entity=target_entity, data=prepped_source_data
                    )
                except MolgenisRequestError as exception:
                    raise ValueError(exception)

    # import contents from a national node entity to the eric entity (combined table)
    def __import_national_node_to_eric_entity(self, node: Node, entity_name):
        """
        Import all national node data into the combined eric entities
        """

        print(f"Importing data for {node.code} on {self._root_url}\n")

        source = self.__get_data_for_entity(entity_name=entity_name, node=node)
        target = self.__get_data_for_entity(entity_name)

        valid_ids = [
            source_id
            for source_id in source["ids"]
            if bbmri_validations.validate_bbmri_id(
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

            source_references = molgenis_utilities.get_all_references_for_entity(
                session=self, entity=source["name"]
            )

            print("Importing data to", target["name"])
            prepped_source_data = (
                molgenis_utilities.transform_to_molgenis_upload_format(
                    data=valid_entries, one_to_manys=source_references["one_to_many"]
                )
            )

            try:
                molgenis_utilities.bulk_add_all(
                    session=self, entity=target["name"], data=prepped_source_data
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

                cached_data = self.__combined_entity_cache[entity_name]
                original_data = _filter_national_node_data(data=cached_data, node=node)
                ids_to_revert = molgenis_utilities.get_all_ids(data=prepped_source_data)

                if len(ids_to_revert) > 0:
                    molgenis_utilities.remove_rows(
                        session=self, entity=target["name"], ids=ids_to_revert
                    )

                if len(original_data) > 0:
                    molgenis_utilities.bulk_add_all(
                        session=self, entity=target["name"], data=original_data
                    )
                    print(
                        f"Rolled back {target['name']} with previous data for "
                        f"{node.code}"
                    )

    def __delete_national_node_own_entity_data(self, node: ExternalNode):
        """
        Delete data before import from national node entity
        """
        print(f"Deleting data for staging area {node.code} on {self._root_url}")

        previous_ids_per_entity = {}

        for entity_name in reversed(self.__IMPORT_SEQUENCE):
            target_entity = self.__get_qualified_entity_name(
                entity_name=entity_name, node=node
            )
            target_data = molgenis_utilities.get_all_rows(
                session=self, entity=target_entity
            )
            ids = molgenis_utilities.get_all_ids(target_data)
            previous_ids_per_entity[entity_name] = ids

            if len(ids) > 0:
                # delete from node specific
                print("Deleting data in", target_entity)
                try:
                    molgenis_utilities.remove_rows(
                        session=self, entity=target_entity, ids=ids
                    )
                except ValueError as exception:
                    raise exception

        return previous_ids_per_entity

    def __prepare_deletion_of_node_data(self):
        """
        Checks the cache and makes one if not found
        """
        # varify we have it cached, if not start caching
        if not all(
            entity_name in self.__combined_entity_cache
            for entity_name in self.__IMPORT_SEQUENCE
        ):
            self.__cache_combined_entity_data()

        for global_entity in self.__TABLES_TO_CACHE:
            source_data = self.__combined_entity_cache[global_entity]
            source_ids = molgenis_utilities.get_all_ids(source_data)
            molgenis_utilities.remove_rows(
                session=self, entity=global_entity, ids=source_ids
            )

    def __delete_national_node_data_from_eric_entity(self, node: Node, entity_name):
        """
        Surgically delete all national node data from combined entities
        """
        # sanity check
        if entity_name not in self.__combined_entity_cache:
            self.__cache_combined_entity_data()

        print(f"\nRemoving data from the entity: {entity_name} for: " f"{node.code}")
        entity_cached_data = self.__combined_entity_cache[entity_name]
        target_entity = self.__get_qualified_entity_name(entity_name=entity_name)
        national_node_data_for_entity = _filter_national_node_data(
            data=entity_cached_data, node=node
        )
        ids_for_national_node_data = molgenis_utilities.get_all_ids(
            data=national_node_data_for_entity
        )

        if len(ids_for_national_node_data) > 0:
            try:
                molgenis_utilities.remove_rows(
                    session=self,
                    entity=target_entity,
                    ids=ids_for_national_node_data,
                )
                print("Removed:", len(ids_for_national_node_data), "rows")
            except ValueError as exception:
                raise exception
        else:
            print("Nothing to remove for ", target_entity)
            print()

    def __replace_global_entities(self):
        """
        Function to loop over global entities and place back the data
        """
        print("Placing back the global entities")
        for global_entity in self.__TABLES_TO_CACHE:
            source_data = self.__combined_entity_cache[global_entity]
            if len(source_data) > 0:
                molgenis_utilities.bulk_add_all(
                    session=self, entity=global_entity, data=source_data
                )
                print(f"Placed back: {len(source_data)} rows to {global_entity}")
            else:
                print("No rows found to place back")
