from dataclasses import dataclass, field
from pprint import pprint
from typing import List

from molgenis.bbmri_eric import _validation
from molgenis.bbmri_eric._model import NodeData, Table
from molgenis.bbmri_eric._validation import ValidationException
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import Node
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
                node_data = self.session.get_node_staging_data(node)
            except MolgenisRequestError:
                # TODO add warning
                continue
            else:
                result = _validation.validate_node(node_data)
                report.add_validation_errors(result.errors)

                try:
                    self._publish(node_data)
                    pass
                except PublishingException as e:
                    report.add_error(e)

        if report.has_errors():
            # TODO raise program failed error
            pass

        pprint(report)

    def _publish(self, node_data: NodeData):
        try:
            self._publish_table(node_data.persons, "eu_bbmri_eric_persons")
            self._publish_table(node_data.networks, "eu_bbmri_eric_networks")
            self._publish_table(node_data.biobanks, "eu_bbmri_eric_biobanks")
            self._publish_table(node_data.collections, "eu_bbmri_eric_collections")
        except MolgenisRequestError as e:
            raise PublishingException(e.message)

    def _publish_table(self, table: Table, production_id: str):
        self.session.upsert_batched(production_id, table.rows)
        # self._delete_rows(table, node)

    def _delete_rows(self, data: Table, node: Node):
        # TODO
        #  1. Deletes rows from the production table that are not present in staging
        #  2. Don't delete the row if it is referred to from the quality tables, raise
        #     a warning instead
        pass
