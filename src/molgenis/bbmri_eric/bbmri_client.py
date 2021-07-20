import json
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
from urllib.parse import quote_plus

import requests

from molgenis.bbmri_eric import _utils
from molgenis.bbmri_eric._model import Node, NodeData, Table, TableType
from molgenis.bbmri_eric._utils import batched
from molgenis.client import Session


@dataclass(frozen=True)
class ReferenceAttributeNames:
    xrefs: List[str]
    mrefs: List[str]
    categoricals: List[str]
    categorical_mrefs: List[str]
    one_to_manys: List[str]


class ReferenceType(Enum):
    XREF = "XREF"
    MREF = "MREF"
    CATEGORICAL = "CATEGORICAL"
    CATEGORICAL_MREF = "CATEGORICAL_MREF"
    ONE_TO_MANY = "ONE_TO_MANY"


class BbmriSession(Session):
    """
    BBMRI Session Class, which extends the molgenis py client Session class
    """

    def __init__(self, url: str, token: Optional[str] = None):
        super().__init__(url, token)
        self.url = url

    def get_node_data(self, node: Node, staging: bool) -> NodeData:
        tables = dict()
        for table_type in TableType.get_import_order():
            if staging:
                id_ = node.get_staging_id(table_type)
            else:
                id_ = table_type.base_id

            tables[table_type] = Table.of(
                table_type=table_type,
                full_name=id_,
                rows=self.get_uploadable_data(id_),
            )

        return NodeData(
            node=node,
            is_staging=staging,
            persons=tables[TableType.PERSONS],
            networks=tables[TableType.NETWORKS],
            biobanks=tables[TableType.BIOBANKS],
            collections=tables[TableType.COLLECTIONS],
        )

    def get_uploadable_data(self, entity_type_id: str) -> List[dict]:
        """
        Returns all the rows of an entity type, transformed to the uploadable format.
        """
        rows = self.get(entity_type_id, batch_size=10000)
        ref_names = self.get_reference_attribute_names(entity_type_id)
        return _utils.transform_to_molgenis_upload_format(rows, ref_names.one_to_manys)

    def get_reference_attribute_names(self, id_: str) -> ReferenceAttributeNames:
        """
        Gets the names of all reference attributes of an entity type

        Parameters:
            id_ (str): the id of the entity type
        """
        attrs = self.get_entity_meta_data(id_)["attributes"]

        result = defaultdict(list)
        for name, attr in attrs.items():
            try:
                type_ = ReferenceType[attr["fieldType"]]
                result[type_].append(name)
            except KeyError:
                pass

        return ReferenceAttributeNames(
            xrefs=result.get(ReferenceType.XREF, []),
            mrefs=result.get(ReferenceType.MREF, []),
            categoricals=result.get(ReferenceType.CATEGORICAL, []),
            categorical_mrefs=result.get(ReferenceType.CATEGORICAL_MREF, []),
            one_to_manys=result.get(ReferenceType.ONE_TO_MANY, []),
        )

    def upsert_batched(self, entity_type_id: str, entities: List[dict]):
        """
        Upserts entities in an entity type (in batches, if needed).
        @param entity_type_id: the id of the entity type to upsert to
        @param entities: the entities to upsert
        """
        meta = self.get_entity_meta_data(entity_type_id)
        id_attr = meta["idAttribute"]
        existing_entities = self.get(
            entity_type_id, batch_size=10000, attributes=id_attr
        )
        existing_ids = {entity[id_attr] for entity in existing_entities}

        add = list()
        update = list()
        for entity in entities:
            if entity[id_attr] in existing_ids:
                update.append(entity)
            else:
                add.append(entity)

        self.add_batched(entity_type_id, add)
        self.update_batched(entity_type_id, update)

    def update(self, entity_type_id: str, entities: List[dict]):
        """Updates multiple entities."""
        response = self._session.put(
            self._api_url + "v2/" + quote_plus(entity_type_id),
            headers=self._get_token_header_with_content_type(),
            data=json.dumps({"entities": entities}),
        )

        try:
            response.raise_for_status()
        except requests.RequestException as ex:
            self._raise_exception(ex)

        return response

    def update_batched(self, entity_type_id: str, entities: List[dict]):
        """Updates multiple entities in batches of 1000."""
        # TODO updating things in bulk will fail if there are self-references across
        #  batches. Dependency resolving is needed.
        batches = list(batched(entities, 1000))
        for batch in batches:
            self.update(entity_type_id, batch)

    def add_batched(self, entity_type_id: str, entities: List[dict]):
        """Adds multiple entities in batches of 1000."""
        # TODO adding things in bulk will fail if there are self-references across
        #  batches. Dependency resolving is needed.
        batches = list(batched(entities, 1000))
        for batch in batches:
            self.add_all(entity_type_id, batch)
