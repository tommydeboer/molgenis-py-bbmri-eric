from typing import Dict, List, Set

from molgenis.bbmri_eric import _enrichment
from molgenis.bbmri_eric._model import Node, NodeData, Table, TableType, get_id_prefix
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.errors import EricError, EricWarning
from molgenis.bbmri_eric.printer import Printer
from molgenis.client import MolgenisRequestError


class Publisher:
    def __init__(self, session: BbmriSession, printer: Printer):
        self.session = session
        self.printer = printer
        self.warnings: List[EricWarning] = []
        self.quality_info: Dict[TableType, Set[str]] = self._get_quality_info()

    def publish(self, node_data: NodeData) -> List[EricWarning]:
        """
        Publishes data from the provided node to the production tables.
        """
        self.warnings = []
        self.printer.print(f"✏️ Enriching data of node {node_data.node.code}")
        _enrichment.enrich_node(node_data)

        self.printer.print(f"✉️ Publishing data of node {node_data.node.code}")
        self._publish_node_data(node_data)
        return self.warnings

    def _publish_node_data(self, node_data: NodeData):
        self.printer.indent()
        for table in node_data.import_order:
            self.printer.print(f"Upserting rows in {table.type.base_id}")
            try:
                self.session.upsert_batched(table.type.base_id, table.rows)
            except MolgenisRequestError as e:
                raise EricError(f"Error upserting rows to {table.type.base_id}") from e

        for table in reversed(node_data.import_order):
            self.printer.print(f"Deleting rows in {table.type.base_id}")
            try:
                self._delete_rows(table, node_data.node)
            except MolgenisRequestError as e:
                raise EricError(f"Error deleting rows from {table.type.base_id}") from e
        self.printer.dedent()

    def _delete_rows(self, table: Table, node: Node):
        # Compare the ids from staging and production to see what was deleted
        staging_ids = {row["id"] for row in table.rows}
        production_ids = self._get_production_ids(table, node)
        deleted_ids = production_ids.difference(staging_ids)

        # Remove ids that we are not allowed to delete
        undeletable_ids = self.quality_info.get(table.type, {})
        deletable_ids = deleted_ids.difference(undeletable_ids)

        # Actually delete the rows in the combined tables
        if deletable_ids:
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
                    self.warnings.append(warning)

    def _get_production_ids(self, table: Table, node: Node) -> Set[str]:
        rows = self.session.get(table.type.base_id, batch_size=10000, attributes="id")
        return {
            row["id"]
            for row in rows
            if row["id"].startswith(get_id_prefix(table.type, node))
        }

    def _get_quality_info(self) -> Dict[TableType, Set[str]]:
        biobanks = self.session.get(
            "eu_bbmri_eric_bio_qual_info", batch_size=10000, attributes="id,biobank"
        )
        collections = self.session.get(
            "eu_bbmri_eric_col_qual_info", batch_size=10000, attributes="id,collection"
        )
        biobank_ids = {row["biobank"]["id"] for row in biobanks}
        collection_ids = {row["collection"]["id"] for row in collections}

        return {TableType.BIOBANKS: biobank_ids, TableType.COLLECTIONS: collection_ids}
