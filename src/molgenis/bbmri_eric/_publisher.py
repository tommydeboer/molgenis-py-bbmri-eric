from dataclasses import dataclass, field
from typing import Dict, List, Set

from molgenis.bbmri_eric import _validation
from molgenis.bbmri_eric._model import Node, NodeData, Table, TableType, get_id_prefix
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
    def __init__(self, session: BbmriSession):
        self.session = session
        self.quality_info: Dict[TableType, Set[str]] = self._get_quality_info()

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

                print(f"Enriching data of node {node.code}")
                self._enrich(node_data)

                for error in result.errors:
                    print(error)

                try:
                    print()
                    print(f"Publishing staging data of node {node.code}")
                    self._publish(node_data)
                except PublishingException as e:
                    report.add_error(e)

        if report.has_errors():
            for error in report.errors:
                print(error)
            pass

    @staticmethod
    def _enrich(node_data: NodeData):
        for collection in node_data.collections.rows:

            def is_true(row: dict, attr: str):
                return attr in row and row[attr] is True

            biobank_id = collection["biobank"]
            biobank = node_data.biobanks.rows_by_id[biobank_id]

            collection["commercial_use"] = (
                is_true(biobank, "collaboration_commercial")
                and is_true(collection, "collaboration_commercial")
                and is_true(collection, "sample_access_fee")
                and is_true(collection, "image_access_fee")
                and is_true(collection, "data_access_fee")
            )

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
        # Compare the ids from staging and production to see what was deleted
        staging_ids = {row["id"] for row in table.rows}
        production_ids = self._get_production_ids(table, node)
        deleted_ids = production_ids.difference(staging_ids)

        # Remove ids that we are not allowed to delete
        undeletable_ids = self.quality_info.get(table.type, {})
        deletable_ids = deleted_ids.difference(undeletable_ids)

        if deletable_ids:
            self.session.delete_list(table.type.base_id, list(deletable_ids))

        if deleted_ids != deletable_ids:
            for id_ in undeletable_ids:
                if id_ in deleted_ids:
                    print(
                        ConstraintViolation(
                            f"Prevented the deletion of a row that is referenced from "
                            f"the quality info: {table.type.value} {id_}."
                        )
                    )

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
