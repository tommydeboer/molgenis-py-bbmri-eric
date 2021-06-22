from dataclasses import dataclass
from typing import List, Set

from molgenis.bbmri_eric import _model, _utils, _validation
from molgenis.bbmri_eric._model import Table
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import Node


class Publisher:
    # TODO move to model.py
    _TABLES_TO_CACHE = [
        "eu_bbmri_eric_bio_qual_info",
        "eu_bbmri_eric_col_qual_info",
    ]

    @dataclass(frozen=True)
    class TableData:
        name: str
        rows: List[dict]
        ids: Set[str]

    def __init__(self, session: BbmriSession):
        self.session = session
        self._cache = {}

    def publish(self, national_nodes: List[Node]):
        """
        Publishes data from the provided nodes to the production tables.
        """
        self._cache = {}
        self._cache_production_tables()
        try:

            for table in reversed(_model.get_import_sequence()):
                for node in national_nodes:
                    self._clear_node_from_production_tables(node, table)

            for table in _model.get_import_sequence():
                print("\n")
                for node in national_nodes:
                    self._publish_node(node, table)
                    print("\n")
        finally:
            self._replace_global_entities()

    def _cache_production_tables(self) -> None:
        """
        Caches data for all bbmri entities, in case of rollback
        """
        for global_entity in self._TABLES_TO_CACHE:
            source_data = self.session.get_all_rows(entity=global_entity)
            source_one_to_manys = self.session.get_one_to_manys(entity=global_entity)
            uploadable_source = _utils.transform_to_molgenis_upload_format(
                data=source_data, one_to_manys=source_one_to_manys
            )

            self._cache[global_entity] = uploadable_source

    def _clear_node_from_production_tables(self, node: Node, table: Table):
        """
        Surgically delete all data of one national node from the production tables
        """
        print(f"\nRemoving data from the entity: {table.name} for: " f"{node.code}")
        all_rows = self._cache[table.name]
        target_entity = table.get_fullname()
        node_rows = _utils.filter_national_node_data(data=all_rows, node=node)
        ids = _utils.get_all_ids(node_rows)

        if len(ids) > 0:
            try:
                self.session.remove_rows(
                    entity=target_entity,
                    ids=ids,
                )
                print("Removed:", len(ids), "rows")
            except ValueError as exception:
                raise exception
        else:
            print("Nothing to remove for ", target_entity)
            print()

    # TODO split up this large method
    def _publish_node(self, node: Node, table: Table):
        """
        Import all data of one national node into the production tables
        """

        print(f"Importing data for {node.code} on {self.session.url}\n")

        source = self._get_data(table, node)
        target = self._get_data(table)

        valid_ids = [
            source_id
            for source_id in source.ids
            if _validation.validate_bbmri_id(
                entity=table.name, node=node, bbmri_id=source_id
            )
        ]

        # check for target ids because there could be eric
        # leftovers from the national node in the table.
        valid_entries = [
            valid_row
            for valid_row in source.rows
            if valid_row["id"] in valid_ids and valid_row["id"] not in target.ids
        ]

        # check the ids per entity if they exist
        # > molgenis_utilities.get_all_ref_ids_by_entity

        if len(valid_entries) > 0:

            source_references = self.session.get_all_references_for_entity(
                entity=source.name
            )

            print("Importing data to", target.name)
            prepped_source_data = _utils.transform_to_molgenis_upload_format(
                data=valid_entries, one_to_manys=source_references["one_to_many"]
            )

            try:
                self.session.bulk_add_all(entity=target.name, data=prepped_source_data)
                print(
                    f"Imported: {len(prepped_source_data)} rows to {target.name}"
                    f"out of {len(source.ids)}"
                )
            except ValueError as exception:  # rollback
                print("\n")
                print("---" * 10)
                print("Failed to import, following error occurred:", exception)
                print("---" * 10)

                cached_data = self._cache[table.name]
                original_data = _utils.filter_national_node_data(
                    data=cached_data, node=node
                )
                ids_to_revert = _utils.get_all_ids(data=prepped_source_data)

                if len(ids_to_revert) > 0:
                    self.session.remove_rows(entity=target.name, ids=ids_to_revert)

                if len(original_data) > 0:
                    self.session.bulk_add_all(entity=target.name, data=original_data)
                    print(
                        f"Rolled back {target.name} with previous data for "
                        f"{node.code}"
                    )

    def _get_data(self, table: Table, node: Node = None) -> TableData:
        """
        Get's data for entity, if national node code is provided, it will fetch data
        from it's own entity
        """
        if node:
            name = table.get_staging_name(node)
        else:
            name = table.get_fullname()

        rows = self.session.get_all_rows(name)
        ids = _utils.get_all_ids(rows)

        return self.TableData(name, rows, ids)

    def _replace_global_entities(self):
        """
        Function to loop over global entities and place back the data
        """
        print("Placing back the global entities")
        for global_entity in self._TABLES_TO_CACHE:
            source_data = self._cache[global_entity]
            if len(source_data) > 0:
                self.session.bulk_add_all(entity=global_entity, data=source_data)
                print(f"Placed back: {len(source_data)} rows to {global_entity}")
            else:
                print("No rows found to place back")
