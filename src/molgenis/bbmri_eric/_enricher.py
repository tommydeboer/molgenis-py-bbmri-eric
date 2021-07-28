from molgenis.bbmri_eric._model import NodeData
from molgenis.bbmri_eric._printer import Printer


class Enricher:
    def __init__(self, node_data: NodeData, printer: Printer):
        self.node_data = node_data
        self.printer = printer

    def enrich(self):
        """
        Enriches the data of a node:
        1. Sets the commercial use boolean
        2. Adds the national node code to all rows
        """
        self._set_commercial_use_bool()
        self._set_national_node_code()

    def _set_commercial_use_bool(self):
        """
        Takes the data of a Node and sets the commercial_use boolean of all collections
        based on a set of criteria.
        """

        self.printer.print("Setting 'commercial_use' booleans")
        for collection in self.node_data.collections.rows:

            def is_true(row: dict, attr: str):
                return attr in row and row[attr] is True

            biobank_id = collection["biobank"]
            biobank = self.node_data.biobanks.rows_by_id[biobank_id]

            collection["commercial_use"] = (
                is_true(biobank, "collaboration_commercial")
                and is_true(collection, "collaboration_commercial")
                and is_true(collection, "sample_access_fee")
                and is_true(collection, "image_access_fee")
                and is_true(collection, "data_access_fee")
            )

    def _set_national_node_code(self):
        """
        Adds the national node code to each row of every table of a node.
        """
        self.printer.print("Adding national node codes")
        for table in self.node_data.import_order:
            for row in table.rows:
                row["national_node"] = self.node_data.node.code
