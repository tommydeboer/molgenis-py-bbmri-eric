from dataclasses import dataclass, field
from pprint import pprint
from typing import List

from molgenis.bbmri_eric import _validation
from molgenis.bbmri_eric._model import Table
from molgenis.bbmri_eric._validation import ValidationException, ValidationState
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
            node_data = self.session.get_node_staging_data(node)
            result = _validation.validate_node(node_data)
            report.add_validation_errors(result.errors)

            try:
                self._publish(node, result)
                pass
            except PublishingException as e:
                report.add_error(e)

        if report.has_errors():
            # TODO raise program failed error
            for error in report.validation_errors:
                print(error)
                print()
            pass

        pprint(report)

    def _publish(self, node: Node, state: ValidationState):
        try:
            pass
        except MolgenisRequestError as e:
            raise PublishingException(e.message)

    def _publish_table(self, table_data: Table, node: Node):
        self._upsert_rows(table_data, node)
        self._delete_rows(table_data, node)

    def _upsert_rows(self, data: Table, node: Node):
        # TODO adds or updates rows in the production tables
        pass

    def _delete_rows(self, data: Table, node: Node):
        # TODO
        #  1. Deletes rows from the production table that are not present in staging
        #  2. Don't delete the row if it is referred to from the quality tables, raise
        #     a warning instead
        pass
