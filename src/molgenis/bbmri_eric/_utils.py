from typing import List


def to_upload_format(rows: List[dict], one_to_manys: List[str] = None) -> List[dict]:
    """
    Changes the output of the REST Client such that it can be uploaded again:
    1. Non-data fields are removed (_href and _meta).
    2. One to manys are removed
    3. Reference objects are removed and replaced with their identifiers.
    """
    upload_format = []
    for row in rows:
        # Remove non-data fields
        row.pop("_href", None)
        row.pop("_meta", None)

        if one_to_manys:
            # Remove one to manys
            for one_to_many in one_to_manys:
                row.pop(one_to_many, None)

        for attr in row:
            if type(row[attr]) is dict:
                # Change xref dicts to id
                ref = row[attr]["id"]
                row[attr] = ref
            elif type(row[attr]) is list and len(row[attr]) > 0:
                # Change mref list of dicts to list of ids
                mref = [ref["id"] for ref in row[attr]]
                row[attr] = mref

        upload_format.append(row)
    return upload_format


def batched(list_: List, batch_size: int):
    """Yield successive n-sized batches from list_."""
    for i in range(0, len(list_), batch_size):
        yield list_[i : i + batch_size]
