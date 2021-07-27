# molgenis-py-bbmri-eric

MOLGENIS Python tooling for BBMRI-ERIC.

## Description
This library contains tools for the MOLGENIS BBMRI-ERIC Directory that help with
staging and publishing the data of the national nodes.

## Usage

These tools can be used as a library in a script or as a command line tool. In both
cases, start by installing the library with `pip install molgenis-py-bbmri-eric`.

### On the command line

The command line tool is called `eric`. Here are some usage examples:

Stage all external nodes:
```
eric stage all
```

Publish nodes UK and NL:
```
eric publish uk nl
```

Use another server as the directory:
```
eric stage be --target <URL_TO_DIRECTORY>
```

Please read the help page (`eric -h`) for more information.

### In a script

```python
from molgenis.bbmri_eric.bbmri_client import BbmriSession
from molgenis.bbmri_eric.eric import Eric

# First, initialise a BbmriSession (an extension of the molgenis-py-client Session)
session = BbmriSession(url="<DIRECTORY_URL>")
session.login("<USERNAME>", "<PASSWORD>")

# Get the nodes you want to work with
nodes = session.get_nodes() # all nodes

# Create an Eric instance and use that to perform the desired actions
eric = Eric(session)
report = eric.publish_nodes(nodes)

if report.has_errors():
    raise ValueError("Some nodes did not publish correctly")
```


## For developers
This project uses [pre-commit](https://pre-commit.com/) and [pipenv](https://pypi.org/project/pipenv/) for the development workflow.

```
# install pre-commit and pipenv if you haven't already
pip install pre-commit
pip install pipenv

# install the git commit hooks
pre-commit install

# create an environment and install the package including all (dev) dependencies
pipenv install

# enter the environment
pipenv shell

# build and run the tests
tox

# the package's command-line entry point is already installed
eric -h
```


## Note

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.
