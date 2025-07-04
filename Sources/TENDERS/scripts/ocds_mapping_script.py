import pandas as pd
import os
from datetime import date

today = date.today()

# Path to the folder containing raw CSVs
# raw_csv_folder = r'/home/prajna/.../tender_data_csv'

# List of required columns matching data_prep_ocds_mapping.py
required_columns = [
    'Tender ID :', 'Tender Title :', 'Work Description', 'Organisation Chain', 'Title',
    'Tender Value in ₹', 'Tender Ref No :', 'Publish Date', 'Bid Validity(Days)',
    'Is Multi Currency Allowed For BOQ', 'Bid Opening Date', 'Tender Category',
    'Tender Type', 'Form of contract', 'Product Category', 'Allow Two Stage Bidding',
    'Allow Preferential Bidder', 'Payment Mode', 'Status',
    'Contract Date :', 'Awarded Value'
]

# Fields to be treated as integers
int_fields = ['Bid Validity(Days)']

def safe_int_convert(value):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def process_tenders_data(raw_csv_folder):
    # 1. Create empty DataFrame with required columns
    data_for_upload = pd.DataFrame(columns=required_columns)

    # 2. Iterate over each CSV in the folder
    for file_name in os.listdir(raw_csv_folder):
        if not file_name.endswith('.csv'):
            continue

        file_path = os.path.join(raw_csv_folder, file_name)
        print(f"Processing: {file_name}")
        raw_data = pd.read_csv(file_path, dtype=str)

        # 3. Extract relevant data into a dict
        tender_info = {}
        for column in required_columns:
            if column in int_fields:
                # exact-match for integer fields
                matches = [col for col in raw_data.columns if col.strip() == column]
                if matches:
                    col = matches[0]
                    non_null = raw_data[col].dropna()
                    val = non_null.iloc[0] if not non_null.empty else None
                    tender_info[column] = safe_int_convert(val)
                else:
                    tender_info[column] = None
            else:
                # substring match for everything else
                cols = raw_data.columns[raw_data.columns.str.contains(column, case=False, na=False)]
                if len(cols):
                    series = raw_data[cols[0]].dropna()
                    val = series.iloc[0] if not series.empty else ''
                    tender_info[column] = val.strip() if isinstance(val, str) else val
                else:
                    tender_info[column] = ''

        # 4. **Append** this tender's info as a new row **inside** the loop**
        data_for_upload.loc[len(data_for_upload)] = tender_info

    # 5. Post-processing (status dedupe, department split, bid counts, etc.)
    data_for_upload['Status'] = data_for_upload['Status'].drop_duplicates(keep='first')
    data_for_upload['department'] = data_for_upload['Organisation Chain'].str.split("|", expand=True)[0]
    data_for_upload.dropna(axis=0, how='all', inplace=True)

    data_temp = data_for_upload[['Tender ID :']].copy()
    data_temp['Tender ID :'] = data_temp['Tender ID :'].fillna(method='ffill')
    bid_counts = data_temp.groupby("Tender ID :").size().reset_index(name='no of bids received')
    data_for_upload = data_for_upload.merge(bid_counts, on='Tender ID :', how='left')

    # 6. Build final OCDS-mapped DataFrame
    today_str = today.isoformat()
    mapping = {
        'ocid': "ocds-f5kvwu-" + data_for_upload['Tender ID :'],
        'initiationType': "tender",
        'tag': "tender",
        'id': 1,
        'date': today_str,
        'tender/id': data_for_upload['Tender ID :'],
        'tender/externalReference': data_for_upload['Tender Ref No :'],
        'tender/title': data_for_upload['Tender Title :'],
        'tender/mainProcurementCategory': data_for_upload['Tender Category'],
        'tender/procurementMethod': data_for_upload['Tender Type'],
        'tender/contractType': data_for_upload['Form of contract'],
        'tenderclassification/description': data_for_upload['Product Category'],
        'tender/submissionMethodDetails': "",
        'tender/participationFee/0/multiCurrencyAllowed': data_for_upload['Is Multi Currency Allowed For BOQ'],
        'tender/allowTwoStageTender': data_for_upload["Allow Two Stage Bidding"],
        'tender/value/amount': data_for_upload['Tender Value in ₹'],
        'tender/datePublished': data_for_upload['Publish Date'],
        'tender/tenderPeriod/durationInDays': data_for_upload['Bid Validity(Days)'],
        'tender/allowPreferentialBidder': data_for_upload['Allow Preferential Bidder'],
        'Payment Mode': data_for_upload['Payment Mode'],
        'tender/status': data_for_upload['Status'],
        'tender/stage': "",
        'tender/numberOfTenderers': data_for_upload['no of bids received'],
        'tender/bidOpening/date': data_for_upload['Bid Opening Date'],
        'tender/milestones/type': "assessment",
        'tender/milestones/title': "Price Bid Opening Date",
        'tender/milestones/dueDate': "",
        'tender/awardedAmount': data_for_upload['Awarded Value'],
        'tender/documents/id': "",
        'buyer/name': data_for_upload['department']
    }

    data_to_upload_final = pd.DataFrame(mapping)

    # 7. Compute Fiscal Year (Apr-Mar)
    data_to_upload_final['Fiscal Year'] = (
        pd.to_datetime(data_to_upload_final['tender/bidOpening/date'])
          .dt.to_period('Q-APR').dt.qyear
          .apply(lambda x: f"{x-1}-{x}")
    )

    return data_to_upload_final

if __name__ == "__main__":
    final_df = process_tenders_data("/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/scripts/tender_data_csv")
    output_path = r"/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/scripts/ocds_transformed_tender_data/ocds_transformed_tender_date.csv"
    final_df.to_csv(output_path, index=False)
    print(f"CSV created successfully at {output_path}")
