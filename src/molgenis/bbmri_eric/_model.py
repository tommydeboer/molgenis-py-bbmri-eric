from dataclasses import dataclass

from molgenis.bbmri_eric.nodes import Node


@dataclass(frozen=True)
class Table:
    # TODO make node part of Table

    name: str
    package = "eu_bbmri_eric"

    def get_fullname(self):
        return f"{self.package}_{self.name}"

    def get_staging_name(self, node: Node):
        return f"{self.package}_{node.code}_{self.name}"


persons = Table("persons")
networks = Table("networks")
biobanks = Table("biobanks")
collections = Table("collections")


def get_import_sequence():
    return [persons, networks, biobanks, collections]


# TODO def get_import_sequence(Node)
