import os
import json
import csv
from datetime import datetime

def convert_timestamp(ms):
    try:
        return datetime.utcfromtimestamp(int(ms) / 1000).strftime('%Y-%m-%d')
    except:
        return None

def extract_template_field(templates, field_code):
    for template in templates:
        for field in template.get("templateFieldList", []):
            if field.get("code") == field_code:
                return convert_timestamp(field.get("value"))
    return None

def process_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        data = json.load(file)

    tender = {
        'tenderid': data.get('currenttenderid') or data.get('tenderid'),
        'tenderrefno': data.get('currenttenderrefno') or data.get('tenderrefno'),
        'description': data.get('currentdescription') or data.get('description'),
        'pacamt': data.get('currentpacamt') or data.get('pacamt'),
        'tendercurrency': data.get('currenttendercurrency') or data.get('tendercurrency'),
        'publishdate': convert_timestamp(data.get('currentTenderPublishDate') or data.get('publishdate')),
        'bid_start_date': convert_timestamp(data.get('currentbidStartDate')),
        'bid_end_date': convert_timestamp(data.get('currentbidEndDate')),
        'bid_open_date': convert_timestamp(data.get('currentbidOpenDate')),
        'doc_sub_date': convert_timestamp(data.get('currentDocSubmissionEndDate')),
        'issuing_authority': data.get('currentorgid') or data.get('orgId') or 'Unknown',
        'procurement_category': data.get('currentproccatid'),
        'tender_type': data.get('currenttendertypeid'),
        'tender_category': data.get('currenttendercatid'),
    }

    return tender

def process_all_to_individual_csvs(folder_paths, output_dir='tender_csvs'):
    os.makedirs(output_dir, exist_ok=True)

    for folder in folder_paths:
        for file in os.listdir(folder):
            if file.endswith('.json'):
                filepath = os.path.join(folder, file)
                try:
                    tender_data = process_json(filepath)
                    tender_id = tender_data.get('tenderid', 'unknown')
                    out_path = os.path.join(output_dir, f"tender_{tender_id}.csv")

                    with open(out_path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=tender_data.keys())
                        writer.writeheader()
                        writer.writerow(tender_data)

                    print(f"Saved {out_path}")
                except Exception as e:
                    print(f"Error processing {file}: {e}")

if __name__ == "__main__":
    process_all_to_individual_csvs([r'D:\CDL\flood-data-ecosystem-Bihar\Sources\TENDERS\scripts\tender_data_json\full_data', r'D:\CDL\flood-data-ecosystem-Bihar\Sources\TENDERS\scripts\tender_data_json\tender_data'])
