from enum import Enum
from typing import List, Set

from molgenis.bbmri_eric.model import OntologyTable


class Category(Enum):
    """
    Enum of Collection Categories with identifiers found in the
    eu_bbmri_eric_collection_category table.
    """

    PAEDIATRIC = "paediatric_only"
    PAEDIATRIC_INCLUDED = "paediatric_included"
    RARE_DISEASE = "rare_disease"
    COVID19 = "covid19"
    CANCER = "cancer"


CANCER_TERMS = {
    "urn:miriam:icd:C00-C97",
    "urn:miriam:icd:D00-D09",
    "urn:miriam:icd:D37-D48",
}

COVID_TERMS = {
    "urn:miriam:icd:U09",
    "urn:miriam:icd:U08",
    "urn:miriam:icd:U11",
    "urn:miriam:icd:U10",
    "urn:miriam:icd:U12",
    "urn:miriam:icd:U07.1",
    "urn:miriam:icd:U07.2",
}


class CategoryMapper:
    def __init__(self, diseases: OntologyTable):
        self.diseases = diseases

    def map(self, collection: dict) -> List[str]:
        """
        Maps data from a collection to a list of categories that the collection belongs
        to.
        :param collection: the collection to map
        :return: a list of categories
        """
        categories = []

        self._map_paediatric(collection, categories)
        self._map_diseases(collection, categories)

        return categories

    @classmethod
    def _map_paediatric(cls, collection: dict, categories: List[str]):
        age_low, age_high = cls._get_ages(collection)
        if age_high < 18:
            categories.append(Category.PAEDIATRIC.value)
        elif age_low < 18:
            categories.append(Category.PAEDIATRIC_INCLUDED.value)

    @staticmethod
    def _get_ages(collection: dict) -> (int, int):
        age_low = collection.get("age_low", None)
        age_high = collection.get("age_high", None)
        if age_low and age_low == 0:
            age_low = None
        if age_high and age_high == 0:
            age_high = None
        return age_low, age_high

    def _map_diseases(self, collection: dict, categories: List[str]):
        diagnoses = collection.get("diagnosis_available", [])
        if diagnoses:
            if self._contains_orphanet(diagnoses):
                categories.append(Category.RARE_DISEASE.value)

            if self._contains_descendant_of(diagnoses, CANCER_TERMS):
                categories.append(Category.CANCER.value)

            if self._contains_descendant_of(diagnoses, COVID_TERMS):
                categories.append(Category.COVID19.value)

    def _contains_orphanet(self, diagnoses: List[str]) -> bool:
        for diagnosis in diagnoses:
            term = self.diseases.rows_by_id.get(diagnosis, None)
            if term and term.get("ontology", "") == "orphanet":
                return True

    def _contains_descendant_of(self, diagnoses: List[str], terms: Set[str]):
        for diagnosis in diagnoses:
            if self.diseases.is_descendant_of_any(diagnosis, terms):
                return True
