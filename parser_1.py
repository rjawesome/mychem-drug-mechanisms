import json
import requests
import math
import csv
import os

def to_list(val):
  if isinstance(val, list):
    return val
  return [val]

def get_target_mapping(data_folder):
  mappings = {}
  with open(os.path.join(data_folder, 'DOWNLOAD-jgNyZYuEBIx0bG8GAt-xS_ow1uKyAG_KbF-6Ar_P7BI=.tsv'), 'r', encoding='utf-8') as f:
    data = csv.reader(f, delimiter='\t')

    for row in list(data)[1:]:
      if row[2] == '':
        continue
      accessions = row[2].split('|')
      if len(accessions) == 1:
        mappings[row[0]] = row[2]
      else:
        mappings[row[0]] = accessions
  return mappings


def load_data(data_folder):
  target_mapping = get_target_mapping(data_folder)

  start = 0

  while True:
    web_data = requests.get(f"https://mychem.info/v1/query?q=_exists_:%22chembl.drug_mechanisms%22&size=1000&from={start}")
    data = web_data.json()

    if len(data['hits']) == 0:
      break

    for c in data['hits']:
      chembl_list = c['chembl']
      for chem in to_list(chembl_list):
        for drug in chem.get('drug_mechanisms', []):
          sbj = {
            "drug_chembl_id": chem["molecule_chembl_id"]
          }
          assoc = {
            "action_type": drug["action_type"],
            "binding_site_name": drug["binding_site_name"],
          }
          if "references" in drug:
            assoc['references'] = drug["references"]

          obj = {
            "target_chembl_id": drug["target_chembl_id"],
          }
          if "target_uniprot_accession" in drug:
            obj["target_uniprot_accession"] = drug["target_uniprot_accession"]
          elif obj["target_chembl_id"] in target_mapping:
            obj["target_uniprot_accession"] = target_mapping[obj["target_chembl_id"]]

          yield {
            'subject': sbj,
            'object': obj,
            'association': assoc
          }

    start += 1000

# for testing parser
if __name__ == '__main__':
  sz = 0
  for d in load_data('./'):
    if sz % 1000 == 0:
      print(sz)
      print(d)
    sz += 1