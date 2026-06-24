import os
import glob
import json
import csv
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_FOLDER = r"D:\CDL\Bihar Scraper\full_data_2026"
OUTPUT_CSV = r"D:\CDL\Bihar Scraper\tenders_full_data_2026.csv"

# CSV headers (normal text like tender_concatenating_utils.py)
CSV_HEADERS = [
    "Tender ID :",
    "Tender Ref No :",
    "Tender Title :",
    "Work Description",
    "Tender Value in ₹",
    "Tender Currency",
    "Bid Currency",
    "Publish Date",
    "Contract Date :",
    "Update Date",
    "Issuing Authority Name",
    "Issuing Authority Designation",
    "Issuing Organisation Name",
#    "Issuing Organisation Code",
    "Issuing Address",
#    "Issuing Email",
#    "Issuing Contact No",
    "Approving Authority Name",
    "Approving Authority Designation",
    "Approving Organisation Name",
#    "Approving Organisation Code",
    "EMD Amount",
    "Tender Fee Amount",
    "Tender Processing Fee Amount",
]

# Map CSV headers -> extracted keys (like TOP_LEVEL_MAP style)
KEY_MAP = {
    "Tender ID :"                   : "tender id",
    "Tender Ref No :"               : "tenderrefno",
    "Tender Title :"                : "title",
    "Work Description"              : "description",
    "Tender Value in ₹"             : "procurement_amount_pacamt",
    "Tender Currency"               : "tendercurrency",
    "Bid Currency"                  : "bidcurrency",
    "Publish Date"                  : "publishdate_iso",
    "Contract Date :"               : "createdate_iso",
    "Update Date"                   : "updatedate_iso",
    "Issuing Authority Name"        : "issuing_authority_name",
    "Issuing Authority Designation" : "issuing_authority_designation",
    "Issuing Organisation Name"     : "issuing_organization_name",
#    "Issuing Organisation Code"     : "issuing_organization_code",
    "Issuing Address"               : "issuing_address",
#    "Issuing Email"                 : "issuing_email",
#    "Issuing Contact No"            : "issuing_contact_no",
    "Approving Authority Name"      : "approving_authority_name",
    "Approving Authority Designation": "approving_authority_designation",
    "Approving Organisation Name"   : "approving_organization_name",
#    "Approving Organisation Code"   : "approving_organization_code",
    "EMD Amount"                    : "emd_amount",
    "Tender Fee Amount"             : "tender_fee_amount",
    "Tender Processing Fee Amount"  : "tender_processing_fee_amount",
}
def ms_to_iso(ms):
    if ms is None:
        return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return ""

def extract_payment_info(templates):
    payment_info = {
        "EMD": "",
        "Tender Fee": "",
        "Tender Processing Fee": "",
    }

    if not isinstance(templates, list):
        return {
            "emd_amount": "",
            "tender_fee_amount": "",
            "tender_processing_fee_amount": "",
        }

    for tmpl in templates:
        if tmpl.get("subProcessName") != "Payment":
            continue

        payment_type = None
        amount = None

        for field in tmpl.get("templateFieldList", []):
            if field.get("code") == "payment_type":
                payment_type = field.get("value")
            elif field.get("code") == "amount":
                amount = field.get("value")

        if payment_type in payment_info:
            payment_info[payment_type] = amount or ""

    return {
        "emd_amount": payment_info["EMD"],
        "tender_fee_amount": payment_info["Tender Fee"],
        "tender_processing_fee_amount": payment_info["Tender Processing Fee"],
    }

def extract_basic_info(data):
    title = data.get("title") or data.get("description") or ""

    tia = data.get("tenderIssuingAuthority", {}) or {}
    taa = data.get("tenderApprovingAuthority", {}) or {}

    payments = extract_payment_info(data.get("templates", []))

    return {
        "tender id": data.get("tenderid", ""),
        "tenderrefno": data.get("tenderrefno", ""),
        "title": title,
        "description": data.get("description", ""),
        "procurement_amount_pacamt": data.get("pacamt", ""),
        "tendercurrency": data.get("tendercurrency", ""),
        "bidcurrency": data.get("bidcurrency", ""),

        "createdate_iso": ms_to_iso(data.get("createdate")),
        "updatedate_iso": ms_to_iso(data.get("updatedate")),
        "publishdate_iso": ms_to_iso(data.get("publishdate")),

        "issuing_authority_name": tia.get("tenderIssuingAuthorityName", ""),
        "issuing_authority_designation": tia.get("tenderIssuingAuthorityDesignation", ""),
        "issuing_organization_name": tia.get("organizationName", ""),
    #    "issuing_organization_code": tia.get("organizationCode", ""),
        "issuing_address": tia.get("address", ""),
    #    "issuing_email": tia.get("email", ""),
    #    "issuing_contact_no": tia.get("contactNo", ""),

        "approving_authority_name": taa.get("tenderApprovingAuthorityName", ""),
        "approving_authority_designation": taa.get("tenderApprovingAuthorityDesignation", ""),
        "approving_organization_name": taa.get("organizationName", ""),
    #    "approving_organization_code": taa.get("organizationCode", ""),

        "emd_amount": payments["emd_amount"],
        "tender_fee_amount": payments["tender_fee_amount"],
        "tender_processing_fee_amount": payments["tender_processing_fee_amount"],
    }

def to_csv_row(extracted: dict) -> dict:
    row = {}
    for hdr in CSV_HEADERS:
        key = KEY_MAP[hdr]
        row[hdr] = extracted.get(key, "")
    return row

def process_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return []

    rows = []
    if isinstance(data, dict):
        rows.append(to_csv_row(extract_basic_info(data)))
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                rows.append(to_csv_row(extract_basic_info(item)))
    return rows

def main():
    json_files = glob.glob(os.path.join(INPUT_FOLDER, "*.json"))
    if not json_files:
        print("No JSON files found.")
        return

    workers = os.cpu_count() or 8
    print(f"Using {workers} threads")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()

        written = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(process_file, path) for path in json_files]
            for future in as_completed(futures):
                rows = future.result()
                if rows:
                    writer.writerows(rows)
                    written += len(rows)
                    if written % 1000 == 0:
                        print(f"Wrote {written} rows...")

    print(f"Done! Extracted {written} tenders into:")
    print(OUTPUT_CSV)

if __name__ == "__main__":
    main()