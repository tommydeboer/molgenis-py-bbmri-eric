from typing import List


def transform_to_molgenis_upload_format(data, one_to_manys: List[str]):
    upload_format = []
    for item in data:
        new_item = item
        del new_item["_href"]
        for one_to_many in one_to_manys:
            del new_item[one_to_many]
        for key in new_item:
            if type(new_item[key]) is dict:
                ref = new_item[key]["id"]
                new_item[key] = ref
            elif type(new_item[key]) is list:
                if len(new_item[key]) > 0:
                    # get id for each new_item in list
                    mref = [ref["id"] for ref in new_item[key]]
                    new_item[key] = mref
        upload_format.append(new_item)
    return upload_format
