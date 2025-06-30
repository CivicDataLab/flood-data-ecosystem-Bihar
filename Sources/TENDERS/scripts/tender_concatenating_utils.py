import os
import glob
import json
import csv
from datetime import datetime

# 1. define your CSV headers and how they map to JSON
CSV_HEADERS = [
    'Tender ID :', 'Tender Title :', 'Work Description', 'Organisation Chain', 'Title',
    'Tender Value in ₹', 'Tender Ref No :', 'Publish Date', 'Bid Validity(Days)',
    'Is Multi Currency Allowed For BOQ', 'Bid Opening Date', 'Tender Category',
    'Tender Type', 'Form of contract', 'Product Category', 'Allow Two Stage Bidding',
    'Allow Preferential Bidder', 'Payment Mode', 'Status',
    'Contract Date :', 'Awarded Value'
]

# map simple top-level JSON keys
TOP_LEVEL_MAP = {
    'Tender ID :'                      : 'tenderid',
    'Tender Title :'                   : 'nit',
    'Work Description'                 : 'description',
    'Organisation Chain'               : 'queryString',
    'Title'                            : 'nit',
    'Tender Value in ₹'                : 'pacamt',
    'Tender Ref No :'                  : 'tenderrefno',
    'Publish Date'                     : 'publishdate',
    'Bid Validity(Days)'               : 'offerValidity',
    'Is Multi Currency Allowed For BOQ': 'bidcurrency',
    'Tender Category'                  : 'tendercatid',
    'Tender Type'                      : 'tendertypeid',
    'Form of contract'                 : 'proccatid',
    'Product Category'                 : 'deptid',
    'Allow Two Stage Bidding'          : 'bidPartNo',
    'Allow Preferential Bidder'        : 'indentFlag',
    'Status'                           : 'status',
    'Contract Date :'                  : 'createdate',
    # Awarded Value—reuse pacamt if no separate field
    'Awarded Value'                    : 'pacamt',
}

# map CSV headers to the `code` of nested template fields
TEMPLATE_MAP = {
    'Bid Opening Date': 'bid_open_date',
    'Payment Mode'     : 'payment_mode',
}

def ms_to_ddmmyyyy(ms: int) -> str:
    """Convert milliseconds since epoch to DD-MM-YYYY (local time)."""
    try:
        return datetime.fromtimestamp(ms / 1000).strftime('%d-%m-%Y')
    except Exception:
        return ''

def extract_template_field(templates, code):
    """Search all templates for the given code and return its fieldValue or blank."""
    for tpl in templates:
        for fld in tpl.get('templateFieldList', []):
            if fld.get('code') == code:
                return fld.get('fieldValue', '')
    return ''

def json_folder_to_csv(input_folder: str, output_csv: str):
    files = glob.glob(os.path.join(input_folder, '*.json'))
    with open(output_csv, 'w', newline='', encoding='utf-8') as fout:
        writer = csv.DictWriter(fout, fieldnames=CSV_HEADERS)
        writer.writeheader()

        for fn in files:
            with open(fn, 'r', encoding='utf-8') as f:
                data = json.load(f)

            row = {}
            # simple top-level fields
            for hdr, key in TOP_LEVEL_MAP.items():
                val = data.get(key, '')
                # convert dates for publishdate & createdate
                if key in ('publishdate', 'createdate') and isinstance(val, (int, float)):
                    val = ms_to_ddmmyyyy(val)
                row[hdr] = val

            # nested/template fields
            templates = data.get('templates', [])
            for hdr, code in TEMPLATE_MAP.items():
                row[hdr] = extract_template_field(templates, code)

            writer.writerow(row)

if __name__=="__main__":
    json_folder_to_csv(
        
        '/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/scripts/tender_data_json/unzipped/full_data',
         '/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/scripts/tender_data_csv/all_tenders_bihar.csv'
         
          )
# example usage:
# json_folder_to_csv('/path/to/tender_jsons', 'all_tenders.csv')
