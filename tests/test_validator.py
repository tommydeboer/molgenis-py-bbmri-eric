from typing import List
from unittest.mock import MagicMock

import pytest

from molgenis.bbmri_eric.errors import EricWarning
from molgenis.bbmri_eric.model import Node, NodeData, Source, Table, TableType
from molgenis.bbmri_eric.printer import Printer
from molgenis.bbmri_eric.validation import Validator


@pytest.fixture
def mock_node_data():
    persons_meta = MagicMock()
    persons_meta.id = "eu_bbmri_eric_NL_persons"
    persons = Table.of(
        TableType.PERSONS,
        persons_meta,
        [
            {"id": "bbmri-eric:contactID:NL_valid-person-1"},
            {"id": "bbmri-eric:contactID:NL_valid:person-2"},
            {"id": "bbmri-eric:ID:NL_invalid_classifier"},  # warning
            {"id": "bbmri-eric:contactID:EU_invalid_node_code"},  # warning
            {"id": "bbmri-eric:contactID:NL_invalid_illegal_characters#$&"},  # warning
        ],
    )

    networks_meta = MagicMock()
    networks_meta.id = "eu_bbmri_eric_NL_networks"
    networks = Table.of(
        TableType.NETWORKS,
        networks_meta,
        [
            {
                "id": "bbmri-eric:networkID:NL_valid-network-1",
                "contact": "bbmri-eric:contactID:NL_valid-person-1",
            },
            {
                "id": "bbmri-eric:networkID:NL_valid:network-2",
                "contact": "bbmri-eric:ID:NL_invalid_classifier",  # warning
                "parent_network": ["bbmri-eric:network:NL_valid-network-1"],
            },
            {"id": "bbmri-eric:ID:NL_invalid_classifier"},  # warning
            {
                "id": "bbmri-eric:networkID:BE_invalid_node_code",  # warning
                "parent_network": [
                    "bbmri-eric:network:NL_valid-network-1",
                    "bbmri-eric:ID:NL_invalid_classifier",  # warning
                ],
            },
        ],
    )

    biobanks_meta = MagicMock()
    biobanks_meta.id = "eu_bbmri_eric_NL_biobanks"
    biobanks = Table.of(
        TableType.BIOBANKS,
        biobanks_meta,
        [
            {
                "id": "bbmri-eric:ID:NL_valid-biobank-1",
                "contact": "bbmri-eric:contactID:NL_valid-person-1",
            },
            {
                "id": "bbmri-eric:ID:NL_valid:biobank-2",
                "contact": "bbmri-eric:ID:NL_invalid_classifier",  # warning
                "network": ["bbmri-eric:networkID:NL_valid-network-1"],
            },
            {"id": "bbmri-eric:test:NL_invalid_classifier"},  # warning
            {
                "id": "bbmri-eric:networkID:BE_invalid_node_code",  # warning
                "network": [
                    "bbmri-eric:network:NL_valid-network-1",
                    "bbmri-eric:ID:NL_invalid_classifier",  # warning
                ],
            },
        ],
    )

    collection_meta = MagicMock()
    collection_meta.id = "eu_bbmri_eric_NL_collections"
    collections = Table.of(
        TableType.COLLECTIONS,
        collection_meta,
        [
            {
                "id": "bbmri-eric:ID:NL_valid-collection-1",
                "contact": "bbmri-eric:contactID:NL_valid-person-1",
                "biobank": "bbmri-eric:networkID:NL_valid-biobank-1",
            },
            {
                "id": "bbmri-eric:ID:NL_valid:collection-2",
                "contact": "bbmri-eric:ID:NL_invalid_classifier",  # warning
                "networks": ["bbmri-eric:network:NL_valid-network-1"],
                "biobank": "bbmri-eric:ID:NL_invalid_classifier",  # warning
                "parent_collection": "bbmri-eric:ID:NL_invalid_classifier",  # warning
            },
            {"id": "bbmri-eric:collection:NL_invalid_classifier"},  # warning
            {
                "id": "bbmri-eric:networkID:BE_invalid_node_code",  # warning
                "networks": [
                    "bbmri-eric:network:NL_valid-network-1",
                    "bbmri-eric:ID:NL_invalid_classifier",  # warning
                ],
                "parent_collection": "bbmri-eric:network:NL_valid-collection-1",
            },
        ],
    )

    return NodeData.from_dict(
        Node("NL", "NL"),
        Source.STAGING,
        {
            TableType.PERSONS.value: persons,
            TableType.NETWORKS.value: networks,
            TableType.BIOBANKS.value: biobanks,
            TableType.COLLECTIONS.value: collections,
        },
    )


@pytest.fixture
def validator() -> Validator:
    return Validator(MagicMock(), MagicMock())


def test_validate_id(mock_node_data):
    validator = Validator(mock_node_data, Printer())

    warnings = validator.validate()

    assert warnings == [
        EricWarning(
            message="bbmri-eric:ID:NL_invalid_classifier in entity: "
            "eu_bbmri_eric_NL_persons does not start with "
            "bbmri-eric:contactID:NL_ or bbmri-eric:contactID:EU_"
        ),
        EricWarning(
            message="bbmri-eric:contactID:NL_invalid_illegal_characters#$& in entity: "
            "eu_bbmri_eric_NL_persons contains invalid characters. Only "
            "alphanumerics and -_: are allowed."
        ),
        EricWarning(
            message="bbmri-eric:ID:NL_invalid_classifier in entity: "
            "eu_bbmri_eric_NL_networks does not start with "
            "bbmri-eric:networkID:NL_ or bbmri-eric:networkID:EU_"
        ),
        EricWarning(
            message="bbmri-eric:networkID:BE_invalid_node_code in entity: "
            "eu_bbmri_eric_NL_networks does not start with "
            "bbmri-eric:networkID:NL_ or bbmri-eric:networkID:EU_"
        ),
        EricWarning(
            message="bbmri-eric:test:NL_invalid_classifier in entity: "
            "eu_bbmri_eric_NL_biobanks does not start with bbmri-eric:ID:NL_"
        ),
        EricWarning(
            message="bbmri-eric:networkID:BE_invalid_node_code in entity: "
            "eu_bbmri_eric_NL_biobanks does not start with bbmri-eric:ID:NL_"
        ),
        EricWarning(
            message="bbmri-eric:collection:NL_invalid_classifier in entity: "
            "eu_bbmri_eric_NL_collections does not start with bbmri-eric:ID:NL_"
        ),
        EricWarning(
            message="bbmri-eric:networkID:BE_invalid_node_code in entity: "
            "eu_bbmri_eric_NL_collections does not start with bbmri-eric:ID:NL_"
        ),
        EricWarning(
            message="bbmri-eric:networkID:NL_valid:network-2 references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:networkID:BE_invalid_node_code references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:ID:NL_valid:biobank-2 references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:networkID:BE_invalid_node_code references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:ID:NL_valid:collection-2 references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:ID:NL_valid:collection-2 references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:ID:NL_valid:collection-2 references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
        EricWarning(
            message="bbmri-eric:networkID:BE_invalid_node_code references invalid id: "
            "bbmri-eric:ID:NL_invalid_classifier"
        ),
    ]


@pytest.mark.parametrize(
    "collection,expected",
    [
        (dict(), []),
        ({"id": "0", "age_low": 0, "age_unit": ["YEAR"]}, []),
        (
            {"id": "1", "age_low": 0, "age_unit": ["YEAR", "MONTH"]},
            [EricWarning("Collection 1 has more than one age_unit: ['YEAR', 'MONTH']")],
        ),
        (
            {"id": "2", "age_low": 5},
            [EricWarning("Collection 2 has age_low/age_high without age_unit")],
        ),
        (
            {"id": "3", "age_low": 0, "age_high": 0, "age_unit": ["YEAR"]},
            [
                EricWarning(
                    "Collection 3 has invalid ages: age_low = 0 and age_high = " "0"
                )
            ],
        ),
        (
            {"id": "4", "age_low": 40, "age_high": 20},
            [
                EricWarning("Collection 4 has age_low/age_high without age_unit"),
                EricWarning("Collection 4 has invalid ages: age_low > age_high"),
            ],
        ),
    ],
)
def test_validate_collection_ages(
    validator: Validator, collection: dict, expected: List[EricWarning]
):
    validator._validate_ages(collection)
    assert validator.warnings == expected
