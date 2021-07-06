from dataclasses import dataclass, field
from typing import List, Set

from molgenis.bbmri_eric import _model, _validation
from molgenis.bbmri_eric._validation import ValidationException
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import Node
from molgenis.client import MolgenisRequestError


@dataclass(frozen=True)
class TableData:
    name: str
    rows: List[dict]


@dataclass()
class ValidationState:
    valid_persons: TableData = None
    valid_networks: TableData = None
    valid_biobanks: TableData = None
    valid_collections: TableData = None
    invalid_ids: Set[str] = field(default_factory=lambda: [])
    errors: List[ValidationException] = field(default_factory=lambda: [])


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
            result = self._validate_node(node)
            report.add_validation_errors(result.errors)

            try:
                self._publish(node, result)
                pass
            except PublishingException as e:
                report.add_error(e)

        if report.has_errors():
            # TODO raise program failed error
            pass

        print(report)

    def _validate_node(self, node: Node) -> ValidationState:
        """
        Validates the staging tables of a single node. Will store all valid rows in a
        ValidationState together with any warnings encountered along the way.
        """
        state = ValidationState()

        persons = self._get_data(_model.persons.get_staging_name(node))
        networks = self._get_data(_model.networks.get_staging_name(node))
        biobanks = self._get_data(_model.biobanks.get_staging_name(node))
        collections = self._get_data(_model.collections.get_staging_name(node))

        state.valid_persons = self._validate_table(persons, node, state)
        state.valid_networks = self._validate_table(networks, node, state)
        state.valid_biobanks = self._validate_table(biobanks, node, state)
        state.valid_collections = self._validate_table(collections, node, state)

        return state

    @staticmethod
    def _validate_table(
        table: TableData, node: Node, state: ValidationState
    ) -> TableData:
        valid_rows = list()
        for row in table.rows:
            id_ = row["id"]
            if _validation.validate_bbmri_id(table.name, node, row["id"]):
                # TODO check if references illegal ids
                # TODO add validation errors to the state
                valid_rows.append(row)
            else:
                state.invalid_ids.add(id_)

        return TableData(name=table.name, rows=valid_rows)

    def _publish(self, node: Node, state: ValidationState):
        try:
            self._publish_table(state.valid_persons, node)
            self._publish_table(state.valid_networks, node)
            self._publish_table(state.valid_biobanks, node)
            self._publish_table(state.valid_collections, node)
        except MolgenisRequestError as e:
            raise PublishingException(e.message)

    def _publish_table(self, table_data: TableData, node: Node):
        self._upsert_rows(table_data, node)
        self._delete_rows(table_data, node)

    def _upsert_rows(self, data: TableData, node: Node):
        # TODO adds or updates rows in the production tables
        pass

    def _delete_rows(self, data: TableData, node: Node):
        # TODO
        #  1. Deletes rows from the production table that are not present in staging
        #  2. Don't delete the row if it is referred to from the quality tables, raise
        #     a warning instead
        pass

    def _get_data(self, table_name: str) -> TableData:
        """
        Gets all the rows of a table and wraps it in a TableData object.
        """
        rows = self.session.get_all_rows(table_name)
        return TableData(table_name, rows)
