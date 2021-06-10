# molgenis-py-bbmri-eric

Python tooling for BBMRI-ERIC


## Description

TODO

## For developers
This project requires that you have `pre-commit` and `pipenv` installed.

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
