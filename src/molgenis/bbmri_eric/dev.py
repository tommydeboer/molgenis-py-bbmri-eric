"""
Development only module
"""

from dotenv import dotenv_values

from molgenis.bbmri_eric.bbmri_client import BbmriSession

config = dotenv_values(".env.local")

target = config["TARGET"]
username = config["USERNAME"]
password = config["PASSWORD"]
external_national_nodes = [
    {"national_node": "DE", "source": "https://directory.bbmri.de"},
    {"national_node": "NL", "source": "https://catalogue.bbmri.nl"},
]

bbmri_session = BbmriSession(
    url=target,
    national_nodes=external_national_nodes,
    username=username,
    password=password,
)

bbmri_session.update_external_entities()
bbmri_session.update_eric_entities()
