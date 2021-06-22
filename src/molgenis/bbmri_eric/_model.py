from dataclasses import dataclass

from molgenis.bbmri_eric.nodes import Node


@dataclass(frozen=True)
class Table:
    name: str
    package = "eu_bbmri_eric"

    def get_fullname(self):
        return f"{self.package}_{self.name}"

    def get_staging_name(self, node: Node):
        return f"{self.package}_{node.code}_{self.name}"


_import_sequence = [
    Table("persons"),
    Table("networks"),
    Table("biobanks"),
    Table("collections"),
]


def get_import_sequence():
    return _import_sequence
