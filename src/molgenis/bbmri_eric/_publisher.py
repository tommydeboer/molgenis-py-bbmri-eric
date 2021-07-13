from dataclasses import dataclass, field
from typing import List, Set

from molgenis.bbmri_eric import _validation
from molgenis.bbmri_eric._model import Node, NodeData, Table, get_id_prefix
from molgenis.bbmri_eric._validation import ConstraintViolation
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.client import MolgenisRequestError


class PublishingException(Exception):
    pass


@dataclass
class PublishingReport:
    errors: List[PublishingException] = field(default_factory=lambda: [])
    validation_errors: List[ConstraintViolation] = field(default_factory=lambda: [])

    def add_error(self, error: PublishingException):
        self.errors.append(error)

    def add_validation_errors(self, errors: List[ConstraintViolation]):
        self.validation_errors += errors

    def has_errors(self) -> bool:
        return len(self.errors) > 0 or len(self.validation_errors) > 0


class Publisher:
    # TODO move to model.py?
    _QUALITY_TABLES = [
        "eu_bbmri_eric_bio_qual_info",
        "eu_bbmri_eric_col_qual_info",
    ]

    def __init__(self, session: BbmriSession):
        self.session = session
        self._cache = {}

    def publish(self, nodes: List[Node]):
        """
        Publishes data from the provided nodes to the production tables.
        """
        report = PublishingReport()
        for node in nodes:
            try:
                print(f"Getting staging data of node {node.code}")
                node_data = self.session.get_node_data(node, staging=True)
            except MolgenisRequestError:
                # TODO add warning
                continue
            else:
                print(f"Validating staging data of node {node.code}")
                result = _validation.validate_node(node_data)
                report.add_validation_errors(result.errors)

                for error in result.errors:
                    print(error)

                try:
                    print()
                    print(f"Publishing staging data of node {node.code}")
                    self._publish(node_data)
                    pass
                except PublishingException as e:
                    report.add_error(e)

        if report.has_errors():
            for error in report.errors:
                print(error)
            pass

    def _publish(self, node_data: NodeData):
        try:
            for table in node_data.import_order:
                print(f"  Upserting rows in {table.full_name}")
                self.session.upsert_batched(table.type.base_id, table.rows)
            for table in reversed(node_data.import_order):
                print(f"  Deleting rows in {table.full_name}")
                self._delete_rows(table, node_data.node)
        except MolgenisRequestError as e:
            raise PublishingException(e.message)

    def _delete_rows(self, table: Table, node: Node):
        staging_ids = {row["id"] for row in table.rows}
        production_ids = self._get_production_ids(table, node)
        deleted_ids = production_ids.difference(staging_ids)
        if deleted_ids:
            self.session.delete_list(table.type.base_id, list(deleted_ids))

        # TODO
        #  1. Deletes rows from the production table that are not present in staging
        #  2. Don't delete the row if it is referred to from the quality tables, raise
        #     a warning instead

    def _get_production_ids(self, table: Table, node: Node) -> Set[str]:
        rows = self.session.get(table.type.base_id, batch_size=10000, attributes="id")
        return {
            row["id"]
            for row in rows
            if row["id"].startswith(get_id_prefix(table.type, node))
        }
