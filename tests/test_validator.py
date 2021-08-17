from molgenis.bbmri_eric.errors import EricWarning
from molgenis.bbmri_eric.model import NodeData
from molgenis.bbmri_eric.printer import Printer
from molgenis.bbmri_eric.validation import Validator


def test_validator(node_data: NodeData):
    validator = Validator(node_data, Printer())

    warnings = validator.validate()

    assert warnings == [
        EricWarning(
            "bbmri-eric:contactID:EU_BBMRI-ERIC in entity: eu_bbmri_eric_NO_persons "
            "does not start with bbmri-eric:contactID:NO_"
        ),
        EricWarning(
            "bbmri-eric:networkID:EU_BBMRI-ERIC:networks:COVID19 in entity: "
            "eu_bbmri_eric_NO_networks does not start with bbmri-eric:networkID:NO_"
        ),
        EricWarning(
            "bbmri-eric:networkID:EU_BBMRI-ERIC:networks:CRC-Cohort in entity: "
            "eu_bbmri_eric_NO_networks does not start with bbmri-eric:networkID:NO_"
        ),
        EricWarning(
            "bbmri-eric:networkID:EU_BBMRI-ERIC:networks:COVID19 references invalid "
            "id: bbmri-eric:contactID:EU_BBMRI-ERIC"
        ),
        EricWarning(
            "bbmri-eric:networkID:EU_BBMRI-ERIC:networks:CRC-Cohort references "
            "invalid id: bbmri-eric:contactID:EU_BBMRI-ERIC"
        ),
        EricWarning(
            "bbmri-eric:ID:NO_CoronaTrondelag references invalid id: "
            "bbmri-eric:networkID:EU_BBMRI-ERIC:networks:COVID19"
        ),
    ]
