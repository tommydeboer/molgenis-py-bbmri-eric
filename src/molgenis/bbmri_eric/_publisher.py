from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, Dict, List, Set

from molgenis.bbmri_eric import _enrichment, _validation
from molgenis.bbmri_eric._model import Node, NodeData, Table, TableType, get_id_prefix
from molgenis.bbmri_eric._validation import ValidationState
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.client import MolgenisRequestError


@dataclass
class PublishError:
    message: str


@dataclass
class PublishWarning:
    message: str


@dataclass
class PublishReport:
    validation: ValidationState = None
    errors: DefaultDict[Node, PublishError] = field(
        default_factory=lambda: defaultdict(list)
    )
    warnings: DefaultDict[Node, List[PublishWarning]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def add_error(self, node: Node, error: PublishError):
        self.errors[node] = error

    def add_warning(self, node: Node, warning: PublishWarning):
        self.warnings[node].append(warning)


class Publisher:
    def __init__(self, session: BbmriSession):
        self.session = session
        self.report = PublishReport()
        self.quality_info: Dict[TableType, Set[str]] = self._get_quality_info()

    def publish(self, nodes: List[Node]) -> PublishReport:
        """
        Publishes data from the provided nodes to the production tables.
        """
        for node in nodes:
            try:
                print(f"Publishing node {node.code}")
                self._publish_node(node)
            except MolgenisRequestError as e:
                error = PublishError(
                    f"Publishing of node {node.code} failed: {e.message}"
                )
                print(error.message)
                self.report.add_error(node, error)

        return self.report

    def _publish_node(self, node: Node):
        print(f"Getting staging data of node {node.code}")
        node_data = self.session.get_node_data(node, staging=True)

        print(f"Validating staging data of node {node.code}")
        validation = _validation.validate_node(node_data)
        validation.print_warnings()
        self.report.validation = validation

        print(f"Enriching data of node {node.code}")
        _enrichment.enrich_node(node_data)

        print(f"Publishing data of node {node.code}")
        self._publish_node_data(node_data)

    def _publish_node_data(self, node_data: NodeData):
        for table in node_data.import_order:
            print(f"  Upserting rows in {table.full_name}")
            self.session.upsert_batched(table.type.base_id, table.rows)

        for table in reversed(node_data.import_order):
            print(f"  Deleting rows in {table.full_name}")
            self._delete_rows(table, node_data.node)

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
                    warning = PublishWarning(
                        f"Prevented the deletion of a row that is referenced from "
                        f"the quality info: {table.type.value} {id_}."
                    )
                    print(warning.message)
                    self.report.add_warning(node, warning)

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
