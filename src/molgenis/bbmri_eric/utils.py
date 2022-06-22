from collections import OrderedDict
from typing import List


def to_upload_format(rows: List[dict]) -> List[dict]:
    """
    Changes the output of the REST Client such that it can be uploaded again:
    1. Non-data fields are removed (_href and _meta).
    2. Reference objects are removed and replaced with their identifiers.

    Note: will only work for the BBMRI-ERIC model because it assumes all identifiers are
    named 'id'.
    """
    upload_format = []
    for row in rows:
        # Remove non-data fields
        row.pop("_href", None)
        row.pop("_meta", None)

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


def to_ordered_dict(rows: List[dict]) -> OrderedDict:
    rows_by_id = OrderedDict()
    for row in rows:
        rows_by_id[row["id"]] = row
    return rows_by_id
