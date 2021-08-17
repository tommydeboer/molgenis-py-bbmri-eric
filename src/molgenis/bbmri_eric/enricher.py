from molgenis.bbmri_eric.model import NodeData, QualityInfo, Table
from molgenis.bbmri_eric.printer import Printer


class Enricher:
    def __init__(self, node_data: NodeData, quality: QualityInfo, printer: Printer):
        self.node_data = node_data
        self.quality = quality
        self.printer = printer

    def enrich(self):
        """
        Enriches the data of a node:
        1. Sets the commercial use boolean
        2. Adds the national node code to all rows
        """
        self._set_commercial_use_bool()
        self._set_national_node_code()
        self._set_quality_info()

    def _set_commercial_use_bool(self):
        """
        Takes the data of a Node and sets the commercial_use boolean of all collections
        based on a set of criteria.
        """

        self.printer.print("Setting 'commercial_use' booleans")
        for collection in self.node_data.collections.rows:

            def is_true(row: dict, attr: str):
                # if the value is not entered, it is also considered true
                return attr not in row or row[attr] is True

            biobank_id = collection["biobank"]
            biobank = self.node_data.biobanks.rows_by_id[biobank_id]

            collection["commercial_use"] = is_true(
                biobank, "collaboration_commercial"
            ) and is_true(collection, "collaboration_commercial")

    def _set_national_node_code(self):
        """
        Adds the national node code to each row of every table of a node.
        """
        self.printer.print("Adding national node codes")
        for table in self.node_data.import_order:
            for row in table.rows:
                row["national_node"] = self.node_data.node.code

    def _set_quality_info(self):
        """
        Adds the one_to_many "quality" field to the biobank and collection tables based
        on the quality info tables.
        """
        self.printer.print("Adding quality information")
        self._set_quality_for_table(self.node_data.biobanks)
        self._set_quality_for_table(self.node_data.collections)

    def _set_quality_for_table(self, table: Table):
        for row in table.rows:
            qualities = self.quality.get_qualities(table.type)
            quality_ids = qualities.get(row["id"], [])
            if quality_ids:
                row["quality"] = quality_ids
