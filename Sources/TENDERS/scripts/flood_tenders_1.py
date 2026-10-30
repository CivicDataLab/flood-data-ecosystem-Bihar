import pandas as pd
import os
import re
import sys
import dateutil.parser
import glob

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tender_tagging import classify_response, tag_scheme

# === CONFIG ===
BASE_PATH = os.getcwd()
print(f"Base path is {BASE_PATH}")
TENDER_PATH = os.path.join(BASE_PATH, 'Sources', 'TENDERS')
MONTHLY_TENDERS_PATH = os.path.join(TENDER_PATH, 'data', 'monthly_tenders')
FLOOD_TENDERS_PATH = os.path.join(TENDER_PATH, 'data', 'flood_tenders')
os.makedirs(FLOOD_TENDERS_PATH, exist_ok=True)

# === KEYWORDS ===
POSITIVE_KEYWORDS = [
    'Flood', 'Embankment', 'embkt', 'Relief', 'Erosion', 'SDRF', 'Inundation', 'Hydrology',
    'Silt', 'Siltation', 'Bund', 'Trench', 'Breach', 'Culvert', 'Sluice', 'Dyke',
    'Storm water drain','Emergency','Immediate', 'IM', 'AE','A E', 'AAPDA MITRA',
    'Bridge', "River", "Drain",'Restoration','Protection','irr','irrigation','dam','Nallah', 'nala',
    'Retrofitting','Pond','Pokhari','D/C','Recharge shaft','LFB','RFB'
]
NEGATIVE_KEYWORDS = [
    'Floodlight', 'Flood Light','GAS', 'FIFA', 'pipe','pipes', 'covid','supply',
    'Beautification','Installation'
]

# === COLUMN NAMES ===
# The scraper in bihar_tender_scraper/ names its columns differently from the older
# scraped data. Map both onto one set of names so every downstream script
# (tagging, geocoding, transformer_4) sees the same schema.
COLUMN_ALIASES = {
    'tender id': 'Tender ID :',
    'tenderrefno': 'Tender Ref No :',
    'Title': 'Tender Title :',
    'Issuing Organization Name': 'Issuing Organisation Name',
    'Approving Organization Name': 'Approving Organisation Name',
    'Emd Amount': 'EMD Amount',
}

def normalise_columns(df):
    renames = {}
    for source, canonical in COLUMN_ALIASES.items():
        if source in df.columns and canonical not in df.columns:
            renames[source] = canonical

    # Older data calls the awarded value 'Tender Value in <rupee symbol>'.
    if 'Awarded Value' not in df.columns:
        value_columns = [c for c in df.columns if c.startswith('Tender Value')]
        if value_columns:
            renames[value_columns[0]] = 'Awarded Value'

    return df.rename(columns=renames)

# === UTILS ===
def populate_keyword_dict(keyword_list):
    return {kw: 0 for kw in keyword_list}

def flood_filter(row):
    tender_slug = f"{row.get('Tender Ref No :','')} {row.get('Tender Title :','')} {row.get('Work Description','')}"
    tender_slug = re.sub(r'[^a-zA-Z0-9 \n\.]', ' ', tender_slug)

    pos_kw = populate_keyword_dict(POSITIVE_KEYWORDS)
    neg_kw = populate_keyword_dict(NEGATIVE_KEYWORDS)
    is_flood = False

    for kw in pos_kw:
        count = len(re.findall(rf"\b{kw.lower()}\b", tender_slug.lower()))
        pos_kw[kw] = count
        if count > 0:
            is_flood = True

    for kw in neg_kw:
        count = len(re.findall(rf"\b{kw.lower()}\b", tender_slug.lower()))
        neg_kw[kw] = count
        if count > 0:
            is_flood = False  # override to False if neg keyword found

    return str(is_flood), str(pos_kw), str(neg_kw)

# === PROCESS EACH CSV ===
csvs = glob.glob(os.path.join(MONTHLY_TENDERS_PATH, '*.csv'))

for csv in csvs:
    filename = os.path.basename(csv)
    df = pd.read_csv(csv)
    df = normalise_columns(df)
    df = df.drop_duplicates()

    df[['is_flood_tender', 'positive_keywords_dict', 'negative_keywords_dict']] = df.apply(flood_filter, axis=1, result_type='expand')

    flood_df = df[df['is_flood_tender'] == 'True']

    if flood_df.empty:
        continue

    # === Remove irrelevant departments if present ===
    if 'Department' in flood_df.columns:
        flood_df = flood_df[~flood_df['Department'].isin([
            "Directorate of Agriculture and Assam Seed Corporation",
            "Department of Handloom Textile and Sericulture"
        ])]

    # === MONSOON CLASSIFICATION ===
    for idx, row in flood_df.iterrows():
        try:
            pub_date = dateutil.parser.parse(str(row['Published Date']))
            month = pub_date.month
            if 3 <= month <= 5:
                season = "Pre-Monsoon"
            elif 6 <= month <= 9:
                season = "Monsoon"
            else:
                season = "Post-Monsoon"
            flood_df.at[idx, 'Season'] = season
        except:
            flood_df.at[idx, 'Season'] = ""

    # === SCHEME TAGGING ===
    flood_df['Scheme'] = flood_df.apply(tag_scheme, axis=1)

    # === EROSION FLAG ===
    erosion_kw = ['anti erosion', 'ae', 'a/e', 'a e', 'erosion', 'eroded', 'erroded', 'errosion']
    flood_df['Erosion'] = flood_df.apply(
        lambda row: any(re.search(rf"\b{kw}\b", str(row.get('Work Description', '')).lower()) for kw in erosion_kw),
        axis=1
    )

    # === INFRA CATEGORY ===
    infra_kw = ['road', 'bridge', 'storm water drain', 'drain', 'culvert', 'embankment', 'bund', 'dyke', 'silt', 'sluice', 'breach']
    flood_df['Roads_Bridges_Embkt'] = flood_df.apply(
        lambda row: any(re.search(rf"\b{kw}\b", str(row.get('Work Description', '')).lower()) for kw in infra_kw),
        axis=1
    )

    # === RESPONSE TYPE ===
    # Categories: Relief and Mitigation / Restoration Measures /
    # Preparedness Measures / Others (see tender_tagging.py)
    flood_df['Response Type'] = flood_df.apply(classify_response, axis=1)

    # === SAVE TAGGED FILE ===
    output_csv = os.path.join(FLOOD_TENDERS_PATH, filename)
    flood_df.to_csv(output_csv, index=False, encoding='utf-8')

# === CONCATENATE ALL FLOOD CSVs ===
flood_csvs = glob.glob(os.path.join(FLOOD_TENDERS_PATH, '*.csv'))
dfs = []

for csv in flood_csvs:
    try:
        df = pd.read_csv(csv)
        df['month'] = os.path.basename(csv)[:7]
        dfs.append(df)
    except Exception as e:
        print(f"Skipping {csv} due to error: {e}")

if dfs:
    all_flood_df = pd.concat(dfs, ignore_index=True)
    all_flood_df.to_csv(os.path.join(TENDER_PATH, 'data', 'flood_tenders_all.csv'), index=False)
    print("Combined all flood-tagged tenders into flood_tenders_all.csv")
else:
    print("No flood_tenders files to concatenate.")

