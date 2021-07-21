from molgenis.bbmri_eric._model import Node
from molgenis.bbmri_eric.errors import EricWarning


class Printer:
    def __init__(self):
        self.indents = 0

    def indent(self):
        self.indents += 1

    def dedent(self):
        self.indents = max(0, self.indents - 1)

    def reset(self):
        self.indents = 0

    def print(self, value: str = None):
        if value:
            print(f"{'  ' * self.indents}{value}")
        else:
            print()

    def print_node_title(self, node: Node):
        title = f"🌍 Node {node.code}"
        border = "=" * (len(title) + 1)
        self.print()
        self.print(border)
        self.print(title)
        self.print(border)

    def error(self, message: str):
        self.print(f"❌  {message}")

    def warning(self, warning: EricWarning):
        self.print(f"⚠️  {warning.message}")

    # TODO PublishReport summary
