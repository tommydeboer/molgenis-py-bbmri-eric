"""
Use this script to update the node_data.pkl file that contains test data (the staging
data of node NO).
"""

import pickle

from dotenv import dotenv_values

# noinspection PyProtectedMember
from molgenis.bbmri_eric._model import Node
from molgenis.bbmri_eric.bbmri_client import BbmriSession

# get credentials from .env.local (in this dir) - if this file doesn't exist: create it
config = dotenv_values(".env.local")
target = config["TARGET"]
username = config["USERNAME"]
password = config["PASSWORD"]

# get staging data of node NL
session = BbmriSession(url=target)
session.login(username, password)
node_data = session.get_node_data(Node("NO", "Norway"), staging=True)

# pickle and write the NodeData object to file
file = open("node_data.pkl", "wb")
pickle.dump(node_data, file)
file.close()
