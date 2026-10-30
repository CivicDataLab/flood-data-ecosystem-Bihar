import pandas as pd
import os
import re
from tqdm import tqdm 
import warnings

# Paths are resolved from this file's location so the scripts run from any
# checkout on any OS.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
TENDER_DATA_PATH = os.path.join(REPO_ROOT, 'Sources', 'TENDERS', 'data')
MAPS_PATH = os.path.join(REPO_ROOT, 'Maps', 'br-ids-drr_shapefile')

warnings.filterwarnings("ignore")

# Only the columns this script keys on have to be present. A blanket .dropna()
# also threw away the 1,981 villages (4.6%) whose gp_name happens to be empty.
BR_VILLAGES = pd.read_csv(os.path.join(MAPS_PATH, 'BIHAR_VILLAGES.csv'), encoding='utf-8')
BR_VILLAGES = BR_VILLAGES.dropna(subset=['dtname', 'sdtname', 'block_name'])

# Authoritative block -> sub-district mapping, keyed on (district, block).
# BIHAR_VILLAGES.csv carries both names and the pairing is unambiguous: 534
# (district, block) pairs, none mapping to more than one sdtname, and every one
# of the 517 sdtnames in the geojson is covered. 18.9% of block names are spelt
# differently from their sdtname (KHODAWANDPUR/Khudabandpur, NAWKOTHI/Naokothi,
# FULLIDUMAR/Phulidumar), which is why resolving the two by fuzzy string match
# downstream was dropping tenders. Look the pair up instead of guessing it.
def _norm(value):
    return str(value).strip().upper()


BLOCK_TO_SDTNAME = {
    (_norm(dt), _norm(blk)): sdt
    for dt, blk, sdt in (BR_VILLAGES[['dtname', 'block_name', 'sdtname']]
                         .drop_duplicates().itertuples(index=False))
}
SDTNAME_BY_DISTRICT = {}
for (dt, _blk), sdt in BLOCK_TO_SDTNAME.items():
    SDTNAME_BY_DISTRICT.setdefault(dt, set()).add(sdt)
# BR_BLOCKS was read here and never used; dropping it removes this script's only
# geopandas dependency. The sub-district geometry is needed in transformer_4.py,
# which still reads it.

#BLOCK_HQs = list(OD_BLOCKS[OD_BLOCKS.HQ=='y']['revenue_ci'])

tenders_df  = pd.read_csv(os.path.join(TENDER_DATA_PATH, 'floodtenders_districtgeotagged.csv'), keep_default_na=False)

MASTER_DFs = []
for FOCUS_DISTRICT in tqdm(BR_VILLAGES.dtname.unique()):
    # Create dictionary for FOCUS DISTRICTS
    FOCUSDIST_village_dict = {}
    FOCUSDIST_subdistrict_dict = {}
    FOCUSDIST_block_dict = {}
    FOCUSDIST_gp_dict = {}
    FOCUSDIST_district_dict = {}
    
    for index,row in BR_VILLAGES[BR_VILLAGES.dtname==FOCUS_DISTRICT].iterrows():
        if row["vilnam_soi"]:
            row["vilnam_soi"] = re.sub(r'[^a-zA-Z]', "", row["vilnam_soi"])
            #if row["vilnam_soi"] in VILLAGE_CORRECTION_DICT:
            #    row["vilnam_soi"] = VILLAGE_CORRECTION_DICT[row["vilnam_soi"]]

            FOCUSDIST_village_dict[row["vilnam_soi"]] = {"village_id" : row["object_idid"],
                                                     "block_name" : row["block_name"],
                                                     "block" : row["block_name"],
                                                     "sdtname": row["sdtname"],
                                                     "gp_name": row["gp_name"],
                                                     "dtname" : row["dtname"]}

        # This used to be assigned twice, the second assignment overwriting the
        # first, which dropped the sdtname and left the block dict holding only
        # the district.
        FOCUSDIST_block_dict[row["block_name"]] = {"block": row["block_name"],
                                                   "subdistrict": row["sdtname"],
                                                   "dtname": row["dtname"]}

        # FOCUSDIST_subdistrict_dict was initialised and then never written to,
        # so subdistrict_patterns below was always empty and the sub-district
        # match loop never ran - tender_subdistrict came out blank on all
        # 23,541 rows. Populate it so an sdtname named in the tender text can
        # resolve a block directly.
        FOCUSDIST_subdistrict_dict[row["sdtname"]] = {"subdistrict": row["sdtname"],
                                                      "dtname": row["dtname"]}
        if isinstance(row["gp_name"], str) and row["gp_name"].strip():
            FOCUSDIST_gp_dict[row["gp_name"]] = {"dtname" : row["dtname"]}
        FOCUSDIST_district_dict[row["dtname"]] = True
    
    try:
        del FOCUSDIST_village_dict['RIVER']
        del FOCUSDIST_village_dict['NO']
        del FOCUSDIST_village_dict['TOWN']
    except:
        pass
    
    FOCUSDIST_villages = list(FOCUSDIST_village_dict.keys())
    FOCUSDIST_blocks = list(FOCUSDIST_block_dict.keys())
    FOCUSDIST_subdistricts = list(FOCUSDIST_subdistrict_dict.keys())
    FOCUSDIST_gp = list(FOCUSDIST_gp_dict.keys())

    # List of substrings to remove from GPE names
    substrings_to_remove = ["(pt)", "\n"]
    # Construct the regex pattern by joining the substrings with "|"
    pattern = "|".join(map(re.escape, substrings_to_remove))

    def compile_names(names):
        """Pre-compile one regex per place name, once per district.

        These were previously rebuilt for every tender, which dominated the runtime.
        """
        compiled = []
        for name in names:
            if not isinstance(name, str) or not re.search(r'[a-zA-Z]', name):
                continue
            search_name = re.sub(pattern, " ", name.lower()).strip()
            if not search_name:
                continue
            compiled.append((name, re.compile(r'\b%s\b' % re.escape(search_name))))
        return compiled

    village_patterns = compile_names(FOCUSDIST_villages)
    block_patterns = compile_names(FOCUSDIST_blocks)
    gp_patterns = compile_names(FOCUSDIST_gp)
    subdistrict_patterns = compile_names(FOCUSDIST_subdistricts)

    ## GEO-CODE VILLAGES, BLOCKS, REVENUE-CIRCLES
    tenders_df_FOCUSDISTRICT = tenders_df[tenders_df["DISTRICT_FINALISED"] == FOCUS_DISTRICT]
    for idx, row in tenders_df_FOCUSDISTRICT.iterrows():
        tender_villages = []
        tender_village_id = ""
        tender_block = ""
        tender_gp = ""
        tender_subdistrict = ""
        # Must be reset per tender: leaving it set carried the previous tender's
        # block over to any tender that matches no block of its own.
        tender_block_location = ""

        tender_slug = str(row['Tender Ref No :']) + ' ' + str(row['Tender Title :']) + ' ' + str(row['Work Description'])
        tender_slug = re.sub(r'[^a-zA-Z0-9 \n\.]', ' ', tender_slug).lower()

        # Prefer the longest village name matched, which is the most specific one,
        # rather than whichever happened to be tested last.
        best_village = ""
        for village, village_regex in village_patterns:
            if village_regex.search(tender_slug):
                tender_villages.append(village)
                if len(village) > len(best_village):
                    best_village = village

        if best_village:
            tender_village_id = FOCUSDIST_village_dict[best_village]['village_id']
            tender_block = FOCUSDIST_village_dict[best_village]['block_name']
            tender_subdistrict = FOCUSDIST_village_dict[best_village]['sdtname']

        for block, block_regex in block_patterns:
            if block_regex.search(tender_slug):
                tender_block = block
                tender_block_location = block
                break

        for gp, gp_regex in gp_patterns:
            if gp_regex.search(tender_slug):
                tender_gp = gp
                break

        for subdistrict, subdistrict_regex in subdistrict_patterns:
            if subdistrict_regex.search(tender_slug):
                tender_subdistrict = subdistrict
                break


        tenders_df_FOCUSDISTRICT.loc[idx,'tender_villages'] = str(tender_villages)[1:-1]
        tenders_df_FOCUSDISTRICT.loc[idx,'tender_block'] = tender_block
        tenders_df_FOCUSDISTRICT.loc[idx,'tender_subdistrict'] = tender_subdistrict
        tenders_df_FOCUSDISTRICT.loc[idx,'gp'] = tender_gp
        tenders_df_FOCUSDISTRICT.loc[idx,'tender_block_location'] = tender_block_location
        
    MASTER_DFs.append(tenders_df_FOCUSDISTRICT)  


MASTER_DFs.append(tenders_df[tenders_df["DISTRICT_FINALISED"] == 'NA'])
MASTER_DFs.append(tenders_df[tenders_df["DISTRICT_FINALISED"] == 'CONFLICT'])

MASTER_DF = pd.concat(MASTER_DFs)

#HQ Flag and RC Finalisation
#MASTER_DF['HQ_flag'] = False
MASTER_DF['BLOCK_FINALISED'] = ''
for idx, row in MASTER_DF.iterrows():
    #if row['tender_revenueci_location'] in RC_HQs:
    #    MASTER_DF.loc[idx, 'HQ_flag'] = True
    
    #if row['HQ_flag'] == False:
    MASTER_DF.loc[idx, 'BLOCK_FINALISED'] = row['tender_block_location']

    if row['tender_block_location'] == '':
        MASTER_DF.loc[idx, 'BLOCK_FINALISED'] = row['tender_block']
    
    if row['tender_block_location'] == row['tender_block']:
        MASTER_DF.loc[idx, 'BLOCK_FINALISED'] = row['tender_block']

    # If HQ True AND row['tender_revenueci_location'] != row['tender_revenueci']?

# Resolve the sub-district name the model is keyed on, so the downstream step
# can merge on it directly instead of fuzzy-matching a block name against it.
# Three routes, most reliable first:
#   1. the (district, block) pair looked up in BLOCK_TO_SDTNAME
#   2. an sdtname named in the tender text, if it belongs to that district
#   3. nothing - left blank, and reported below rather than lost silently
_dist_norm = MASTER_DF['DISTRICT_FINALISED'].map(_norm)
_block_norm = MASTER_DF['BLOCK_FINALISED'].map(_norm)

_from_block = [BLOCK_TO_SDTNAME.get((d, b), '') for d, b in zip(_dist_norm, _block_norm)]

_from_text = []
for d, sdt in zip(_dist_norm, MASTER_DF['tender_subdistrict']):
    sdt = str(sdt).strip()
    _from_text.append(sdt if sdt and sdt in SDTNAME_BY_DISTRICT.get(d, ()) else '')

MASTER_DF['SDTNAME_FINALISED'] = [a or b for a, b in zip(_from_block, _from_text)]

MASTER_DF.to_csv(os.path.join(TENDER_DATA_PATH, 'floodtenders_blockgeotagged.csv'))


# === SANITY CHECKS ===
# A finalised block must belong to the finalised district. Violations mean a block
# was attributed to a tender it does not belong to.
block_districts = (BR_VILLAGES[['block_name', 'dtname']].dropna()
                   .assign(block_name=lambda d: d.block_name.str.strip().str.upper(),
                           dtname=lambda d: d.dtname.str.strip().str.upper()))
valid_pairs = set(zip(block_districts.block_name, block_districts.dtname))
known_blocks = set(block_districts.block_name)

finalised_block = MASTER_DF['BLOCK_FINALISED'].astype(str).str.strip().str.upper()
finalised_district = MASTER_DF['DISTRICT_FINALISED'].astype(str).str.strip().str.upper()
checkable = finalised_block.isin(known_blocks)
violations = checkable & ~pd.Series([pair in valid_pairs for pair in zip(finalised_block, finalised_district)],
                                    index=MASTER_DF.index)

print('\nTenders written: ', MASTER_DF.shape[0])
print('Tenders with a block assigned: ', int(checkable.sum()))

# Unresolved tenders are dropped by the groupby in transformer_4.py, so say how
# much money that is here rather than letting it disappear without a trace.
_value = pd.to_numeric(MASTER_DF.get('Awarded Value'), errors='coerce').fillna(0)
_unresolved = MASTER_DF['SDTNAME_FINALISED'].astype(str).str.strip() == ''
print('Sub-district resolved:   {} tenders ({:.1f}%), value {:,.0f}'.format(
    int((~_unresolved).sum()), 100 * float((~_unresolved).mean()), _value[~_unresolved].sum()))
print('Sub-district UNRESOLVED: {} tenders ({:.1f}%), value {:,.0f}'.format(
    int(_unresolved.sum()), 100 * float(_unresolved.mean()), _value[_unresolved].sum()))
if violations.any():
    print('WARNING: {} tenders have a block that belongs to a different district!'.format(int(violations.sum())))
    print(MASTER_DF[violations][['DISTRICT_FINALISED', 'BLOCK_FINALISED']].head(10).to_string())
else:
    print('All assigned blocks belong to their finalised district.')



