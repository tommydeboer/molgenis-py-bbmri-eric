"""
This script updates the biobank and collection quality information on the
German Directory with the quality information from the BBMRI-ERIC Directory.

The script can be placed and scheduled on the German Directory
with the ${molgenisToken} security token activated.
"""

from molgenis.client import Session

tables = {
    "eu_bbmri_eric_biobanks": ["eu_bbmri_eric_bio_qual_info", "biobank"],
    "eu_bbmri_eric_collections": ["eu_bbmri_eric_col_qual_info", "collection"],
}
de_session = Session(url="https://directory.bbmri.de", token="${molgenisToken}")
eric_session = Session(url="https://directory.bbmri-eric.eu")

for table in tables:
    de_quals = []
    qual_table = tables[table][0]
    de_data = de_session.get(table, attributes="id", uploadable=True)
    qual_data = eric_session.get(qual_table, uploadable=True)
    for row in de_data:
        de_quals.extend(
            [qual for qual in qual_data if qual[tables[table][1]] == row["id"]]
        )
    de_session.delete(qual_table)
    de_session.add_all(qual_table, de_quals)
