"""
Development only module
"""

from dotenv import dotenv_values

from molgenis.bbmri_eric import nodes
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.publisher import Publisher
from molgenis.bbmri_eric.stager import Stager

config = dotenv_values(".env.local")

target = config["TARGET"]
username = config["USERNAME"]
password = config["PASSWORD"]

bbmri_session = BbmriSession(url=target)
bbmri_session.login(username, password)

Stager(bbmri_session).stage(nodes.get_all_external_nodes())
Publisher(bbmri_session).publish(nodes.get_all_nodes())
