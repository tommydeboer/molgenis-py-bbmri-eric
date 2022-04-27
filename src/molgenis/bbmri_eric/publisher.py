from dataclasses import dataclass, field
from typing import List

from molgenis.bbmri_eric.bbmri_client import EricSession
from molgenis.bbmri_eric.errors import EricError, EricWarning, ErrorReport
from molgenis.bbmri_eric.model import (
    MixedData,
    Node,
    NodeData,
    QualityInfo,
    Source,
    Table,
    TableType,
)
from molgenis.bbmri_eric.pid_manager import BasePidManager
from molgenis.bbmri_eric.printer import Printer
from molgenis.client import MolgenisRequestError


@dataclass
class PublishingState:
    existing_data: MixedData
    eu_node_data: NodeData
    quality_info: QualityInfo
    nodes: List[Node]
    report: ErrorReport
    data_to_publish: MixedData = field(init=False)

    def __post_init__(self):
        self.data_to_publish = MixedData.from_empty(Source.TRANSFORMED)


class Publisher:
    """
    This class is responsible for copying data from the staging areas to the combined
    public tables.
    """

    def __init__(
        self,
        session: EricSession,
        printer: Printer,
        quality_info: QualityInfo,
        pid_manager: BasePidManager,
    ):
        self.session = session
        self.printer = printer
        self.quality_info = quality_info
        self.pid_manager = pid_manager

    def publish(self, state: PublishingState):
        """
        Copies staging data to the combined tables. This happens in two phases:
        1. New/existing rows are upserted in the combined tables
        2. Removed rows are deleted from the combined tables
        """
        self.printer.print("💾 Saving data to combined tables")
        with self.printer.indentation():
            self._upsert_data(state)

        self.printer.print("🧼 Cleaning up removed data in combined tables")
        with self.printer.indentation():
            self._delete_data(state)

    def _upsert_data(self, state):
        for table in state.data_to_publish.import_order:
            self.printer.print(f"Upserting rows in {table.type.base_id}")
            try:
                self.session.upsert(table.type.base_id, table.rows)
            except MolgenisRequestError as e:
                raise EricError(f"Error upserting rows to {table.type.base_id}") from e

    def _delete_data(self, state):
        for table in reversed(state.data_to_publish.import_order):
            self.printer.print(f"Deleting rows in {table.type.base_id}")
            try:
                with self.printer.indentation():
                    self._delete_rows(
                        table,
                        state.existing_data.table_by_type[table.type],
                        state.report,
                    )
            except MolgenisRequestError as e:
                raise EricError(f"Error deleting rows from {table.type.base_id}") from e

    def _delete_rows(self, table: Table, existing_table: Table, report: ErrorReport):
        """
        Deletes rows from a combined table that are not present in the staging area's
        table. If a row is referenced from the quality info tables, it is not deleted
        but a warning will be raised.

        :param Table table: the staging area's table
        :param Table existing_table: the existing rows
        """
        # Compare the ids from staging and production to see what was deleted
        staging_ids = table.rows_by_id.keys()
        production_ids = set(existing_table.rows_by_id.keys())
        deleted_ids = production_ids.difference(staging_ids)

        # Remove ids that we are not allowed to delete
        undeletable_ids = self.quality_info.get_qualities(table.type).keys()
        deletable_ids = deleted_ids.difference(undeletable_ids)

        # For deleted biobanks, update the handle
        if table.type == TableType.BIOBANKS:
            self.pid_manager.terminate_biobanks(
                [existing_table.rows_by_id[id_]["pid"] for id_ in deletable_ids]
            )

        # Actually delete the rows in the combined table
        if deletable_ids:
            self.printer.print(
                f"Deleting {len(deletable_ids)} row(s) in {table.type.base_id}"
            )
            self.session.delete_list(table.type.base_id, list(deletable_ids))

        # Show warning for every id that we prevented deletion of
        if deleted_ids != deletable_ids:
            for id_ in undeletable_ids:
                if id_ in deleted_ids:
                    warning = EricWarning(
                        f"Prevented the deletion of a row that is referenced from "
                        f"the quality info: {table.type.value} {id_}."
                    )
                    self.printer.print_warning(warning)

                    code = existing_table.rows_by_id[id_]["national_node"]
                    report.add_node_warnings(Node.of(code), [warning])
