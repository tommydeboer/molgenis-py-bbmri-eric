"""
Example usage file meant for development. Make sure you have an .env.local file and a
pyhandle_creds.json file in this folder.
"""
import re
import secrets
import time
import uuid

import shortuuid
from snowflake2 import make_snowflake

default_alphabet = shortuuid.get_alphabet()

id1 = make_snowflake(time.time(), 0, 0, 0)
time.sleep(0.1)
id2 = make_snowflake(time.time(), 0, 0, 1)
time.sleep(0.1)
id3 = make_snowflake(time.time(), 0, 0, 2)


def print_snowflake(id_):
    print("    snowflake: " + id_)

    hex_id = hex(int(id_))[2:].upper()
    print("          hex: " + hex_id)

    formatted_hex_id = re.findall("....?", hex_id)
    print("formatted hex: " + "-".join(formatted_hex_id))
    print()


print_snowflake(id1)
print_snowflake(id2)
print_snowflake(id3)


def print_uuid(alphabet):
    shortuuid.set_alphabet(alphabet)

    uid = uuid.uuid4()
    print("               uuid: " + str(uid))

    short_id = shortuuid.encode(uid)
    print("          shortuuid: " + short_id)

    truncated_id = short_id[:12]
    print("truncated shortuuid: " + truncated_id)

    formatted_id = "-".join(re.findall("....?", truncated_id))
    print("formatted shortuuid: " + formatted_id)

    print()


def print_hex():
    print(secrets.token_hex(6))
    print()


print_uuid(default_alphabet)
print_uuid("0123456789abcdefgh")
print_uuid("0123456789abcdefgh")
print_uuid("0123456789abcdefgh")
print_uuid("0123456789abcdefgh")

print_hex()
