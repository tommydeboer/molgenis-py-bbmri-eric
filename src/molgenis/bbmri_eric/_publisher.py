from dataclasses import dataclass, field
from typing import List, Set

from molgenis.bbmri_eric import _utils, _validation
from molgenis.bbmri_eric._model import Table
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.nodes import Node
from molgenis.client import MolgenisRequestError


@dataclass(frozen=True)
class TableData:
    name: str
    rows: List[dict]
    ids: Set[str]


class ValidationException(Exception):
    pass


@dataclass()
class ValidationState:
    valid_persons: TableData = field(default_factory=lambda: [])
    valid_networks: TableData = field(default_factory=lambda: [])
    valid_biobanks: TableData = field(default_factory=lambda: [])
    valid_collections: TableData = field(default_factory=lambda: [])
    errors: List[ValidationException] = field(default_factory=lambda: [])

    def get_valid_data(self):
        """
        Returns the validated tables in the correct import order.
        """
        return [
            self.valid_persons,
            self.valid_networks,
            self.valid_biobanks,
            self.valid_biobanks,
        ]


class PublishingException(Exception):
    pass


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
            except PublishingException as e:
                report.add_error(e)

        if report.has_errors():
            # TODO raise program failed error
            pass

    def _validate_node(self, node: Node) -> ValidationState:
        """
        Validates the staging tables of a single node. Will store all correct rows in a
        ValidationReport together with any warnings encountered along the way.
        """
        state = ValidationState()

        # TODO for each table:
        #  1. Validate rows
        #  2. Add valid rows to the state
        #  3. Add any warnings/errors to the state

        return state

    def _publish(self, node: Node, state: ValidationState):
        try:
            for table_data in state.get_valid_data():
                self._upsert_rows(node, table_data)
                self._delete_rows(node, table_data)
        except MolgenisRequestError as e:
            raise PublishingException(e.message)

    def _upsert_rows(self, node: Node, data: TableData):
        # TODO adds or updates rows in the production tables
        pass

    def _delete_rows(self, node: Node, data: TableData):
        # TODO
        #  1. Deletes rows from the production table that are not present in staging
        #  2. Don't delete the row if it is referred to from the quality tables, raise
        #     a warning instead
        pass

    def _publish_node_old(self, node: Node, table: Table):
        """
        Import all data of one national node into the production tables
        """
        # TODO this is the old way, remove this method when code has been rearranged
        print(f"Importing data for {node.code} on {self.session.url}\n")

        staging = self._get_data(table.get_staging_name(node))
        production = self._get_data(table.get_fullname())

        valid_ids = [
            source_id
            for source_id in staging.ids
            if _validation.validate_bbmri_id(
                entity=table.name, node=node, bbmri_id=source_id
            )
        ]

        # TODO is this still needed if we are not clearing the tables anymore?
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

    def _get_data(self, table_name: str) -> TableData:
        """
        Gets all the rows of a table and wraps it in a TableData object.
        """
        rows = self.session.get_all_rows(table_name)

        # TODO move this to the TableData class (not always needed)
        ids = _utils.get_all_ids(rows)

        return TableData(table_name, rows, ids)
