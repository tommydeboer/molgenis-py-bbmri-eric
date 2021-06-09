import logging
from typing import List

_logger = logging.getLogger(__name__)


def import_national_nodes(nodes: List[str]):
    _logger.debug("Started importing...")

    for node in nodes:
        print(f"Importing {node}")

    _logger.info("Finished importing all nodes.")
