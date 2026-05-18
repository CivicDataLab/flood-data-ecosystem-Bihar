import os
import glob
import json
import csv
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed



INPUT_FOLDER = r"D:\CDL\Bihar Scraper\full_data"
OUTPUT_CSV = r"D:\CDL\Bihar Scraper\tenders_full_data.csv"



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
        "EMD Amount": payment_info["EMD"],
        "Tender Fee Amount": payment_info["Tender Fee"],
        "Tender Processing Fee Amount": payment_info["Tender Processing Fee"],
    }


def extract_basic_info(data):
    title = data.get("title") or data.get("description") or ""

    tia = data.get("tenderIssuingAuthority", {}) or {}
    taa = data.get("tenderApprovingAuthority", {}) or {}

    payments = extract_payment_info(data.get("templates", []))

    return {
        "tender id": data.get("tenderid", ""),
        "tenderrefno": data.get("tenderrefno", ""),
        "Title": title,
        "Work Description": data.get("description", ""),
        #"groupid": data.get("groupid", ""),
        "Awarded Value": data.get("pacamt", ""),
        "Tender Currency": data.get("tendercurrency", ""),
        "Bid Currency": data.get("bidcurrency", ""),

        "Create Date": ms_to_iso(data.get("createdate")),
        "Update Date": ms_to_iso(data.get("updatedate")),
        "Publish Date": ms_to_iso(data.get("publishdate")),

        # Issuing Authority
        "Issuing Authority Name": tia.get("tenderIssuingAuthorityName", ""),
        "Issuing Authority Designation": tia.get("tenderIssuingAuthorityDesignation", ""),
        "Issuing Organization Name": tia.get("organizationName", ""),
        "Issuing Organization Code": tia.get("organizationCode", ""),
        "Issuing Address": tia.get("address", ""),
        "Issuing Email": tia.get("email", ""),
        "Issuing Contact No": tia.get("contactNo", ""),

        # Approving Authority
        "Approving Authority Name": taa.get("tenderApprovingAuthorityName", ""),
        "Approving Authority Designation": taa.get("tenderApprovingAuthorityDesignation", ""),
        "Approving Organization Name": taa.get("organizationName", ""),
        "Approving Organization Code": taa.get("organizationCode", ""),

        # Payment Details
        "Emd Amount": payments["emd_amount"],
        "Tender Fee Amount": payments["tender_fee_amount"],
        "Tender Processing Fee Amount": payments["tender_processing_fee_amount"],
    }


def process_file(path):
    """Executed in parallel per file""" 
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return []

    rows = []

    if isinstance(data, dict):
        rows.append(extract_basic_info(data))
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                rows.append(extract_basic_info(item))

    return rows


def main():
    json_files = glob.glob(os.path.join(INPUT_FOLDER, "*.json"))
    if not json_files:
        print("No JSON files found.")
        return

    rows = []

    workers = os.cpu_count() or 8
    print(f"Using {workers} threads")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_file, path) for path in json_files]

        for future in as_completed(futures):
            rows.extend(future.result())

    if not rows:
        print("No data extracted.")
        return

    fieldnames = list(rows[0].keys())

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done! Extracted {len(rows)} tenders into:")
    print(OUTPUT_CSV)


if __name__ == "__main__":
    main()
