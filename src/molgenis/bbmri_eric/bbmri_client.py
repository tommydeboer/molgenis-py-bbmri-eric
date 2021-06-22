"""
BBMRI interface for Molgenis
"""
from typing import Optional

from molgenis.client import MolgenisRequestError, Session


class BbmriSession(Session):
    """
    BBMRI Session Class, which extends the molgenis py client Session class
    """

    def __init__(self, url: str, token: Optional[str] = None):
        super().__init__(url, token)
        self.url = url

    def remove_rows(self, entity, ids):
        if len(ids) > 0:
            try:
                self.delete_list(entity, ids)
            except MolgenisRequestError as exception:
                raise ValueError(exception)

    def get_all_rows(self, entity):
        data = []
        while True:
            if len(data) == 0:
                # api can handle 10.000 max per request
                data = self.get(entity=entity, num=10000, start=len(data))
                if len(data) == 0:
                    break  # if the table is empty
            else:
                newdata = self.get(entity=entity, num=10000, start=len(data))
                if len(newdata) > 0:
                    data.extend(data)
                else:
                    break

        return data

    def get_all_references_for_entity(self, entity):
        """retrieves one_to_many and xref attributes"""
        meta = self.get_entity_meta_data(entity)["attributes"]
        one_to_many = [
            attr for attr in meta if meta[attr]["fieldType"] == "ONE_TO_MANY"
        ]
        xref = [attr for attr in meta if meta[attr]["fieldType"] == "XREF"]
        return {"xref": xref, "one_to_many": one_to_many}

    def get_one_to_manys(self, entity):
        """Retrieves one-to-many's in table"""
        all_references = self.get_all_references_for_entity(entity=entity)
        return all_references["one_to_many"]

    def bulk_add_all(self, entity, data):
        if len(data) == 0:
            return

        max_update_count = 1000

        if len(data) <= max_update_count:
            try:
                self.add_all(entity=entity, entities=data)
                return
            except MolgenisRequestError as exception:
                raise ValueError(exception)

        number_of_cycles = int(len(data) / max_update_count)

        try:
            for cycle in range(number_of_cycles):
                next_batch_start = int(cycle * max_update_count)
                next_batch_stop = int(max_update_count + cycle * max_update_count)
                items_to_add = data[next_batch_start:next_batch_stop]
                self.add_all(entity=entity, entities=items_to_add)
        except MolgenisRequestError as exception:
            raise ValueError(exception)
