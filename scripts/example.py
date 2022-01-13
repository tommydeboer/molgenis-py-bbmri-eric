"""
Example usage file meant for development. Make sure you have an .env.local file and a
pyhandle_creds.json file in this folder.
"""

from dotenv import dotenv_values

from molgenis.bbmri_eric.bbmri_client import EricSession
from molgenis.bbmri_eric.eric import Eric
from molgenis.bbmri_eric.pid_service import PidService

# Get credentials from .env.local
config = dotenv_values(".env")
target = config["TARGET"]
username = config["USERNAME"]
password = config["PASSWORD"]

# Login to the directory with an EricSession
session = EricSession(url=target)
session.login(username, password)

# Create PidService
pid_service = PidService.from_credentials("pyhandle_creds.json")
# Use the DummyPidService if you want to test without interacting with a handle server
# pid_service = DummyPidService()

# Instantiate the Eric class and do some work
eric = Eric(session, pid_service)
eric.publish_nodes(session.get_nodes(["CY"]))
