import json
from typing import List, Optional
from urllib.parse import quote_plus

import requests

from molgenis.bbmri_eric import _utils
from molgenis.bbmri_eric._model import (
    ExternalServerNode,
    Node,
    NodeData,
    Table,
    TableType,
)
from molgenis.bbmri_eric._utils import batched
from molgenis.client import Session


class BbmriSession(Session):
    """
    BBMRI Session Class, which extends the molgenis py client Session class
    """

    NODES_TABLE = "eu_bbmri_eric_national_nodes"

    def __init__(self, url: str, token: Optional[str] = None):
        super().__init__(url, token)
        self.url = url

    def get_nodes(self, codes: List[str] = None) -> List[Node]:
        """
        Retrieves a list of Node objects from the national nodes table. Will return
        all nodes or some nodes if 'codes' is specified.
        :param codes: nodes to get by code
        :return: list of Node objects
        """
        if codes:
            nodes = self.get(self.NODES_TABLE, q=f"id=in=({','.join(codes)})")
        else:
            nodes = self.get(self.NODES_TABLE)

        if codes:
            self._validate_codes(codes, nodes)
        return self._to_nodes(nodes)

    def get_external_nodes(self, codes: List[str] = None) -> List[ExternalServerNode]:
        """
        Retrieves a list of ExternalServerNode objects from the national nodes table.
        Will return all nodes or some nodes if 'codes' is specified.
        :param codes: nodes to get by code
        :return: list of ExternalServerNode objects
        """
        if codes:
            nodes = self.get(self.NODES_TABLE, q=f"id=in=({','.join(codes)});dns!=''")
        else:
            nodes = self.get(self.NODES_TABLE, q="dns!=''")

        if codes:
            self._validate_codes(codes, nodes)
        return self._to_nodes(nodes)

    @staticmethod
    def _validate_codes(codes: List[str], nodes: List[dict]):
        """Raises a KeyError if a requested node code was not found."""
        retrieved_codes = {node["id"] for node in nodes}
        for code in codes:
            if code not in retrieved_codes:
                raise KeyError(f"Unknown code: {code}")

    @staticmethod
    def _to_nodes(nodes: List[dict]):
        """Maps rows to Node or ExternalServerNode objects."""
        result = list()
        for node in nodes:
            if "dns" not in node:
                result.append(Node(code=node["id"], description=node["description"]))
            else:
                result.append(
                    ExternalServerNode(
                        code=node["id"],
                        description=node["description"],
                        url=node["dns"],
                    )
                )
        return result

    def get_node_data(self, node: Node, staging: bool) -> NodeData:
        """
        Gets the four tables that belong to a single node. If staging=True, this
        method will fetch the tables from the node's staging area, else it will use the
        base table identifiers.

        :param Node node: the node to get the data for
        :param bool staging: true if the staging data should be retrieved, false if the
                             default tables should be retrieved
        :return: a NodeData object
        """

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
        return _utils.to_upload_format(rows)

    def upsert_batched(self, entity_type_id: str, entities: List[dict]):
        """
        Upserts entities in an entity type (in batches, if needed).
        @param entity_type_id: the id of the entity type to upsert to
        @param entities: the entities to upsert
        """
        # Get the existing identifiers
        meta = self.get_entity_meta_data(entity_type_id)
        id_attr = meta["idAttribute"]
        existing_entities = self.get(
            entity_type_id, batch_size=10000, attributes=id_attr
        )
        existing_ids = {entity[id_attr] for entity in existing_entities}

        # Based on the existing identifiers, decide which rows should be added/updated
        add = list()
        update = list()
        for entity in entities:
            if entity[id_attr] in existing_ids:
                update.append(entity)
            else:
                add.append(entity)

        # Do the adds and updates in batches
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
