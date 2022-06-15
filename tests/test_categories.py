from typing import List
from unittest.mock import MagicMock

import pytest

from molgenis.bbmri_eric.categories import Category, CategoryMapper
from molgenis.bbmri_eric.model import OntologyTable


@pytest.fixture
def mapper():
    return CategoryMapper(MagicMock())


@pytest.fixture
def disease_ontology() -> OntologyTable:
    meta = MagicMock()
    meta.id_attribute = "id"
    return OntologyTable.of(
        meta,
        [
            {"id": "urn:miriam:icd:T18.5"},
            {"id": "urn:miriam:icd:C00-C97", "ontology": "ICD-10"},
            {
                "id": "urn:miriam:icd:C97",
                "parentId": "urn:miriam:icd:C00-C97",
                "ontology": "ICD-10",
            },
            {"id": "urn:miriam:icd:U09"},
            {
                "id": "urn:miriam:icd:U09.9",
                "parentId": "urn:miriam:icd:U09",
            },
            {"id": "ORPHA:93969", "ontology": "orphanet"},
        ],
        "parentId",
    )


@pytest.mark.parametrize(
    "collection,expected",
    [
        (dict(), []),
        ({"diagnosis_available": ["urn:miriam:icd:T18.5"]}, []),
        ({"diagnosis_available": ["ORPHA:93969"]}, [Category.RARE_DISEASE.value]),
        ({"diagnosis_available": ["urn:miriam:icd:C97"]}, [Category.CANCER.value]),
        ({"diagnosis_available": ["urn:miriam:icd:U09.9"]}, [Category.COVID19.value]),
        (
            {
                "diagnosis_available": [
                    "urn:miriam:icd:U09",
                    "urn:miriam:icd:T18.5",
                    "ORPHA:93969",
                ]
            },
            [Category.RARE_DISEASE.value, Category.COVID19.value],
        ),
    ],
)
def test_map_diseases(mapper, disease_ontology, collection: dict, expected: List[str]):
    mapper.diseases = disease_ontology
    categories = []
    mapper._map_diseases(collection, categories)
    assert categories == expected
