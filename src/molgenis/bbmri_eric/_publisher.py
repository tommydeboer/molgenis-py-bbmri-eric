from dataclasses import dataclass
from typing import List, Set

from molgenis.bbmri_eric import _model, _utils, _validation
from molgenis.bbmri_eric._model import Table
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import Node


class Publisher:
    # TODO move to model.py?
    _QUALITY_TABLES = [
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

    def publish(self, nodes: List[Node]):
        """
        Publishes data from the provided nodes to the production tables.
        """
        self._cache = {}
        self._cache_and_clear_quality_tables()
        try:

            for node in nodes:
                for table in reversed(_model.get_import_sequence()):
                    self._clear_node_from_production_tables(node, table)

                for table in _model.get_import_sequence():
                    for node in nodes:
                        self._publish_node(node, table)
        finally:
            # TODO remove?
            self._restore_quality_tables()

    def _cache_and_clear_quality_tables(self) -> None:
        """
        Stores the quality tables in a cache. The quality tables have references to the
        other production tables, so they need to be (temporarily) cleared before you
        can remove and re-add rows to the production tables.
        """
        for quality_table in self._QUALITY_TABLES:
            source_data = self.session.get_all_rows(entity=quality_table)
            ref_names = self.session.get_reference_attribute_names(quality_table)
            uploadable_source = _utils.transform_to_molgenis_upload_format(
                data=source_data, one_to_manys=ref_names.one_to_manys
            )

            self._cache[quality_table] = uploadable_source

            # TODO don't remove the quality data, alternatives:
            #  1. Create backup .csv
            #  2. Make the XREF a STRING <-- not allowed
            #  3. Temporary table
            #  4. Just delete it and hope for the best
            #  5. Don't delete biobanks/collections but update them

            # TODO delete biobank from quality table if a biobank/collection was deleted

            self.session.delete(quality_table)

    def _clear_node_from_production_tables(self, node: Node, table: Table):
        """
        Surgically delete all data of one national node from the production tables
        """
        # TODO incorrect method name
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

    def _publish_node(self, node: Node, table: Table):
        """
        Import all data of one national node into the production tables
        """
        # TODO incorrect method name
        # TODO split up this large method
        print(f"Importing data for {node.code} on {self.session.url}\n")

        staging = self._get_data(table, node)
        production = self._get_data(table)

        valid_ids = [
            source_id
            for source_id in staging.ids
            if _validation.validate_bbmri_id(
                entity=table.name, node=node, bbmri_id=source_id
            )
        ]

        # check for target ids because there could be eric
        # leftovers from the national node in the table.
        valid_entries = [
            valid_row
            for valid_row in staging.rows
            if valid_row["id"] in valid_ids and valid_row["id"] not in production.ids
        ]

        # check the ids per entity if they exist
        # > molgenis_utilities.get_all_ref_ids_by_entity

        if len(valid_entries) > 0:

            ref_names = self.session.get_reference_attribute_names(id_=staging.name)

            print("Importing data to", production.name)
            prepped_source_data = _utils.transform_to_molgenis_upload_format(
                data=valid_entries,
                one_to_manys=ref_names.one_to_manys,
            )

            try:
                self.session.bulk_add_all(
                    entity=production.name, data=prepped_source_data
                )
                print(
                    f"Imported: {len(prepped_source_data)} rows to {production.name}"
                    f"out of {len(staging.ids)}"
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
                    self.session.remove_rows(entity=production.name, ids=ids_to_revert)

                if len(original_data) > 0:
                    self.session.bulk_add_all(
                        entity=production.name, data=original_data
                    )
                    print(
                        f"Rolled back {production.name} with previous data for "
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

    def _restore_quality_tables(self):
        """
        Restores the quality tables.
        """
        # TODO this will fail if some nodes weren't added correctly. Then you might end
        #  up with partially filled quality tables due to the nature of bulk_add_all()
        print("Restoring the quality tables")
        for quality_table in self._QUALITY_TABLES:
            rows = self._cache[quality_table]
            if len(rows) > 0:
                self.session.bulk_add_all(entity=quality_table, data=rows)
                print(f"Placed back: {len(rows)} rows to {quality_table}")
            else:
                print("No rows found to place back")
