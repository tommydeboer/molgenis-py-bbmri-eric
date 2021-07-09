from dataclasses import dataclass, field
from typing import List

from molgenis.bbmri_eric import _validation
from molgenis.bbmri_eric._model import Node, NodeData, Table, TableType
from molgenis.bbmri_eric._validation import ValidationException
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.client import MolgenisRequestError


class PublishingException(Exception):
    pass


@dataclass
class PublishingReport:
    errors: List[PublishingException] = field(default_factory=lambda: [])
    validation_errors: List[ValidationException] = field(default_factory=lambda: [])

    def add_error(self, error: PublishingException):
        self.errors.append(error)

    def add_validation_errors(self, errors: List[ValidationException]):
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
                node_data = self.session.get_node_staging_data(node)
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
            # TODO raise program failed error
            pass

    def _publish(self, node_data: NodeData):
        try:
            self._publish_table(node_data.persons)
            self._publish_table(node_data.networks)
            self._publish_table(node_data.biobanks)
            self._publish_table(node_data.collections)
        except MolgenisRequestError as e:
            raise PublishingException(e.message)

    def _publish_table(self, table: Table):
        print(f"  Publishing table {table.type.value}")
        production_id = self.to_eric_full_name(table.type)
        self.session.upsert_batched(production_id, table.rows)
        # self._delete_rows(table, production_id)

    def _delete_rows(self, table: Table, production_id: str):
        production_rows = self.session.get(
            production_id, batch_size=10000, attributes="id"
        )
        production_ids = {row["id"] for row in production_rows}
        staging_ids = {row["id"] for row in table.rows}

        deleted_ids = staging_ids.difference(production_ids)

        self.session.delete_list(production_ids, list(deleted_ids))

        # TODO
        #  1. Deletes rows from the production table that are not present in staging
        #  2. Don't delete the row if it is referred to from the quality tables, raise
        #     a warning instead
        pass

    @staticmethod
    def to_eric_full_name(table_type: TableType):
        return f"eu_bbmri_eric_{table_type}"

    @staticmethod
    def filter_national_node_data(data: List[dict], node: Node) -> List[dict]:
        """
        Filters data from an entity based on national node code in an Id
        """
        national_node_signature = f":{node.code}_"
        data_from_national_node = [
            row for row in data if national_node_signature in row["id"]
        ]
        return data_from_national_node
