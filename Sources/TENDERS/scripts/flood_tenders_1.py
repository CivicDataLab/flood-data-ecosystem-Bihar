import pandas as pd
import os
import re
import dateutil.parser
import glob

# === CONFIG ===
BASE_PATH = os.getcwd()
TENDER_PATH = os.path.join(BASE_PATH, 'flood-data-ecosystem-Bihar', 'Sources', 'TENDERS')
MONTHLY_TENDERS_PATH = os.path.join(TENDER_PATH, 'monthly_tenders')
FLOOD_TENDERS_PATH = os.path.join(TENDER_PATH, 'data', 'flood_tenders')
os.makedirs(FLOOD_TENDERS_PATH, exist_ok=True)

# === KEYWORDS ===
POSITIVE_KEYWORDS = [
    'Flood', 'Embankment', 'embkt', 'Relief', 'Erosion', 'SDRF', 'Inundation', 'Hydrology',
    'Silt', 'Siltation', 'Bund', 'Trench', 'Breach', 'Culvert', 'Sluice', 'Dyke',
    'Storm water drain','Emergency','Immediate', 'IM', 'AE','A E', 'AAPDA MITRA',
    'Bridge', "River", "Drain",'Restoration','Protection','irr','irrigation','dam','Nallah',
    'Retrofitting','Pond','Pokhari','D/C','Recharge shaft','LFB','RFB'
]
NEGATIVE_KEYWORDS = [
    'Floodlight', 'Flood Light','GAS', 'FIFA', 'pipe','pipes', 'covid','supply',
    'Beautification','Installation'
]

# === UTILS ===
def populate_keyword_dict(keyword_list):
    return {kw: 0 for kw in keyword_list}

def flood_filter(row):
    tender_slug = f"{row.get('tender_externalreference','')} {row.get('tender_title','')} {row.get('Work Description','')}"
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
csvs = glob.glob(os.path.join('/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/monthly_tenders', '*.csv'))

for csv in csvs:
    filename = os.path.basename(csv)
    df = pd.read_csv(csv)
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
    schemes = []
    scheme_kw = {'ridf','sdrf','sopd','cidf','ltif','sdmf','ndrf'}
    for _, row in flood_df.iterrows():
        slug = f"{row.get('tender_title','')} {row.get('tender_externalreference','')} {row.get('Work Description','')}"
        slug = re.sub(r'[^a-zA-Z0-9 \n\.]', ' ', slug).lower()
        slug_tokens = set(re.split(r'[-.,()_\s/]\s*', slug))
        matches = list(slug_tokens & scheme_kw)
        schemes.append(matches[0].upper() if matches else '')
    flood_df['Scheme'] = schemes

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
    immediate_kw = ['sdrf','im','i/m','gr','g/r','relief','immediate', 'emergency']
    repair_kw = ['improvement', 'repair', 'restoration', 'raising', 'strengthening', 'renovation']
    preparedness_kw = ['shelter', 'responder kit', 'aapda mitra', 'protection', 'stockpile']

    def classify_response(row):
        slug = f"{row.get('tender_externalreference','')} {row.get('tender_title','')} {row.get('Work Description','')}"
        slug = re.sub(r'[^a-zA-Z0-9 \n\.]', ' ', slug).lower()
        if any(kw in slug for kw in immediate_kw):
            return "Immediate Measures"
        elif any(kw in slug for kw in repair_kw):
            return "Repair and Restoration"
        elif any(kw in slug for kw in preparedness_kw):
            return "Preparedness Measures"
        return "Others"

    flood_df['Response Type'] = flood_df.apply(classify_response, axis=1)

    # === SAVE TAGGED FILE ===
    output_csv = os.path.join(FLOOD_TENDERS_PATH, filename)
    flood_df.to_csv(output_csv, index=False, encoding='utf-8')
    print(f"✅ Saved flood-tagged file: {output_csv}")

# === CONCATENATE ALL FLOOD CSVs ===
flood_csvs = glob.glob(os.path.join(FLOOD_TENDERS_PATH, '*.csv'))
dfs = []

for csv in flood_csvs:
    try:
        df = pd.read_csv(csv)
        df['month'] = os.path.basename(csv)[:7]
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Skipping {csv} due to error: {e}")

if dfs:
    all_flood_df = pd.concat(dfs, ignore_index=True)
    all_flood_df.to_csv(os.path.join('/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS', 'data', 'flood_tenders_all.csv'), index=False)
    print("✅ Combined all flood-tagged tenders into flood_tenders_all.csv")
else:
    print("⚠️ No flood_tenders files to concatenate.")
