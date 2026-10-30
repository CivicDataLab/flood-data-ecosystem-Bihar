import pandas as pd
import glob
import os
import sys
from rapidfuzz import process, fuzz
import geopandas as gpd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tender_tagging import classify_response, tag_scheme, RESPONSE_TYPE_VARIABLES

# Paths are resolved from this file's location so the scripts run from any
# checkout on any OS.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
TENDER_DATA_PATH = os.path.join(REPO_ROOT, 'Sources', 'TENDERS', 'data')
MAPS_PATH = os.path.join(REPO_ROOT, 'Maps', 'br-ids-drr_shapefile')


def best_match(
    query: str,
    choices:any,
    scorer: fuzz = fuzz.token_sort_ratio,
    score_cutoff: int = 80
):
    """
    Return the best choice for `query` from `choices` if score >= score_cutoff,
    else None.
    """
    # Tenders with no finalised block have nothing to match on. Newer pandas keeps
    # these as NaN through .astype(str), and rapidfuzz returns None for them.
    if not isinstance(query, str) or not query.strip():
        return None
    result = process.extractOne(query, choices, scorer=scorer)
    if result is None:
        return None
    match, score, _ = result
    return match if score >= score_cutoff else None


# data_path = os.getcwd()+'/Sources/TENDERS/data/'
data_path = TENDER_DATA_PATH
od_gdf = gpd.read_file(os.path.join(MAPS_PATH, 'Bihar_Subdistrict_final_simplified.geojson'))

flood_tenders_geotagged_df = pd.read_csv(data_path + '/floodtenders_blockgeotagged.csv', encoding='latin1')

# --- resolve the sub-district each tender belongs to ---
#
# geocode_blocks_3.py now writes SDTNAME_FINALISED, looked up from the
# authoritative (district, block) -> sdtname pairing in BIHAR_VILLAGES.csv.
# Prefer it. Fall back to a fuzzy match only for rows it could not resolve.
#
# The fuzzy fallback is restricted to the sdtnames of the tender's own district.
# Matching against all 517 statewide was self-defeating: the merge below keys on
# (DISTRICT_FINALISED, matched_sdtname), so a block matched to a similarly named
# sub-district in a different district produced a pair that matches nothing and
# the tender was dropped. Restricting the candidates also makes a correct match
# far more likely, since each district holds only ~14 sub-districts.
sdt_by_district = (od_gdf[['dtname', 'sdtname']].dropna()
                   .assign(dtname=lambda d: d.dtname.str.strip().str.upper())
                   .groupby('dtname')['sdtname'].unique().to_dict())


def _clean(value):
    """Trimmed string, or '' for anything pandas treats as null.

    str(value or '') will not do here: a float nan is truthy, so an unresolved
    cell stringifies to 'nan' and then reads as a resolved sub-district name.
    That made every row look resolved and left the fuzzy fallback below
    unreachable.
    """
    if pd.isna(value):
        return ''
    return str(value).strip()


def resolve_sdtname(row):
    resolved = _clean(row.get('SDTNAME_FINALISED'))
    if resolved:
        return resolved
    block = _clean(row.get('BLOCK_FINALISED'))
    if not block:
        return None
    choices = sdt_by_district.get(_clean(row.get('DISTRICT_FINALISED')).upper())
    if choices is None or len(choices) == 0:
        return None
    return best_match(block.title(), list(choices))


if 'SDTNAME_FINALISED' not in flood_tenders_geotagged_df.columns:
    flood_tenders_geotagged_df['SDTNAME_FINALISED'] = ''

flood_tenders_geotagged_df['matched_sdtname'] = flood_tenders_geotagged_df.apply(
    resolve_sdtname, axis=1)

_direct = int(flood_tenders_geotagged_df['SDTNAME_FINALISED'].map(_clean).ne('').sum())
print('sub-district from the authoritative (district, block) lookup: {}'.format(int(_direct)))
print('sub-district from the district-restricted fuzzy fallback:     {}'.format(
    int(flood_tenders_geotagged_df['matched_sdtname'].notna().sum() - _direct)))


flood_tenders_geotagged_df = flood_tenders_geotagged_df.merge( od_gdf[['dtname', 'sdtname', 'object_id', 'geometry']],
    left_on = ['DISTRICT_FINALISED', 'matched_sdtname'],
    right_on = ['dtname', 'sdtname'],
    how     = 'left')

# aggregate() below does groupby(['month', 'object_id']), and pandas drops rows
# whose group key is NaN. Any tender without an object_id therefore leaves the
# pipeline here, silently. Say how much it is.
_v = pd.to_numeric(flood_tenders_geotagged_df['Awarded Value'], errors='coerce').fillna(0)
_lost = flood_tenders_geotagged_df['object_id'].isna()
print('tenders with no object_id (dropped from every variable): {} of {} ({:.1f}%), value {:,.0f} of {:,.0f} ({:.1f}%)'.format(
    int(_lost.sum()), len(_lost), 100 * float(_lost.mean()),
    _v[_lost].sum(), _v.sum(), 100 * _v[_lost].sum() / max(_v.sum(), 1)))
if _lost.any():
    _by_dist = (flood_tenders_geotagged_df[_lost]
                .assign(_v=_v[_lost])
                .groupby('DISTRICT_FINALISED')['_v'].agg(['size', 'sum'])
                .sort_values('sum', ascending=False).head(10))
    print('worst districts for unresolved tenders:')
    print(_by_dist.to_string())


# The awarded value column is named 'Awarded Value' by the current scraper and
# 'Tender Value in <rupee symbol>' in the older scraped data.
value_column = 'Awarded Value'
if value_column not in flood_tenders_geotagged_df.columns:
    value_column = [c for c in flood_tenders_geotagged_df.columns if c.startswith('Tender Value')][0]
    flood_tenders_geotagged_df = flood_tenders_geotagged_df.rename(columns={value_column: 'Awarded Value'})
flood_tenders_geotagged_df['Awarded Value'] = pd.to_numeric(
    flood_tenders_geotagged_df['Awarded Value'], errors='coerce').fillna(0.0)

# Re-tag scheme and response type here as well, so the variables always follow the
# taxonomy in tender_tagging.py even when the geotagged file was written by an
# earlier run of flood_tenders_1.py.
flood_tenders_geotagged_df['Scheme'] = flood_tenders_geotagged_df.apply(tag_scheme, axis=1)
flood_tenders_geotagged_df['Response Type'] = flood_tenders_geotagged_df.apply(classify_response, axis=1)


written_variables = set()


def write_monthly_variable(variable_df, variable):
    """Write one CSV per month into data/variables/<variable>/."""
    variable_dir = os.path.join(data_path, 'variables', variable)
    os.makedirs(variable_dir, exist_ok=True)

    # Clear the previous run's files first. Without this, a month that no longer
    # has tenders for this variable keeps its old CSV, and re-geocoding a tender
    # to a different sub-district leaves the original value behind as well.
    for stale in glob.glob(os.path.join(variable_dir, '*.csv')):
        os.remove(stale)

    written_variables.add(variable)
    for year_month in variable_df.month.unique():
        variable_df_monthly = variable_df[variable_df.month == year_month]
        variable_df_monthly = variable_df_monthly[['object_id', variable]]
        variable_df_monthly.to_csv(
            os.path.join(variable_dir, '{}_{}.csv'.format(variable, year_month)), index=False)
    print('{}: wrote {} monthly files'.format(variable, variable_df.month.nunique()))


def aggregate(df, variable):
    """Sum awarded value by month and sub-district, named after `variable`."""
    aggregated = df.groupby(['month', 'object_id'])[['Awarded Value']].sum().reset_index()
    return aggregated.rename(columns={'Awarded Value': variable})


# --- district-only tenders -------------------------------------------------
#
# Roughly a third of flood tenders name a district but no block anywhere in
# their text (the count and value for this run are printed below), and the
# source carries nothing finer: there is no
# pincode field in the scraped record, and the only location field is the
# issuing office's address, which resolves to district headquarters towns
# ("near town hall, lalbagh, darbhanga"), not to the work site. Attributing
# them to a block would invent precision and would push spend into the HQ block
# of every district.
#
# So they are aggregated to the district instead, and written to a parallel
# variables_district/ tree keyed on the district object_id ('10-209'). The
# block-level variables are left untouched, so no block gains spend it cannot
# be shown to have received.
#
# Note this makes a district's tender total larger than the sum of its blocks.
# That is the point: the difference is money that is known to have been spent
# in the district but cannot be placed within it. District *scores* are
# unaffected either way, since they roll up from the block 1-5 classes rather
# than from rupees.
DISTRICT_OF_BLOCK = {}
for _dt, _oid in od_gdf[['dtname', 'object_id']].dropna().itertuples(index=False):
    parts = str(_oid).split('-')
    if len(parts) >= 2:
        DISTRICT_OF_BLOCK[str(_dt).strip().upper()] = '-'.join(parts[:2])

district_only_df = flood_tenders_geotagged_df[
    flood_tenders_geotagged_df['object_id'].isna()].copy()
district_only_df['object_id'] = (district_only_df['DISTRICT_FINALISED']
                                 .astype(str).str.strip().str.upper()
                                 .map(DISTRICT_OF_BLOCK))
district_only_df = district_only_df[district_only_df['object_id'].notna()]

_dv = pd.to_numeric(district_only_df['Awarded Value'], errors='coerce').fillna(0)
print('district-only tenders: {} rows, value {:,.0f} -> variables_district/'.format(
    len(district_only_df), _dv.sum()))


def write_monthly_variable_district(variable_df, variable):
    """Write one CSV per month into data/variables_district/<variable>/."""
    variable_dir = os.path.join(data_path, 'variables_district', variable)
    os.makedirs(variable_dir, exist_ok=True)
    for stale in glob.glob(os.path.join(variable_dir, '*.csv')):
        os.remove(stale)
    for year_month in variable_df.month.unique():
        rows = variable_df[variable_df.month == year_month][['object_id', variable]]
        rows.to_csv(os.path.join(variable_dir, '{}_{}.csv'.format(variable, year_month)),
                    index=False)
    print('{} (district): wrote {} monthly files'.format(variable, variable_df.month.nunique()))


# Total tender variable
write_monthly_variable(aggregate(flood_tenders_geotagged_df, 'total_tender_awarded_value'),
                       'total_tender_awarded_value')

if not district_only_df.empty:
    write_monthly_variable_district(
        aggregate(district_only_df, 'total_tender_awarded_value'),
        'total_tender_awarded_value')

# Scheme wise tender variables: sdrf_tenders_awarded_value, ridf_tenders_awarded_value, ...
for scheme in flood_tenders_geotagged_df['Scheme'].unique():
    if not str(scheme).strip() or str(scheme) == 'nan':
        continue
    variable = str(scheme).lower() + '_tenders_awarded_value'
    scheme_df = flood_tenders_geotagged_df[flood_tenders_geotagged_df['Scheme'] == scheme]
    write_monthly_variable(aggregate(scheme_df, variable), variable)
    scheme_dist = district_only_df[district_only_df['Scheme'] == scheme]
    if not scheme_dist.empty:
        write_monthly_variable_district(aggregate(scheme_dist, variable), variable)

# Response type wise tender variables
for response_type, variable in RESPONSE_TYPE_VARIABLES.items():
    response_df = flood_tenders_geotagged_df[flood_tenders_geotagged_df['Response Type'] == response_type]
    if response_df.empty:
        print('{}: no tenders tagged, skipped'.format(variable))
        continue
    write_monthly_variable(aggregate(response_df, variable), variable)
    response_dist = district_only_df[district_only_df['Response Type'] == response_type]
    if not response_dist.empty:
        write_monthly_variable_district(aggregate(response_dist, variable), variable)

# Any variable folder not written by this run holds data from an older run and
# would still be picked up by master.py.
existing = {os.path.basename(p) for p in glob.glob(os.path.join(data_path, 'variables', '*'))
            if os.path.isdir(p)}
for stale_variable in sorted(existing - written_variables):
    print('WARNING: {} was not regenerated by this run and may be stale'.format(stale_variable))

# import pandas as pd
# import os
# import geopandas as gpd

# # === CONFIG ===
# DATA_PATH = '/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/data'
# SHAPEFILE = '/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Maps/br-ids-drr_shapefile/Bihar_Subdistrict_final_simplified.geojson'
# GEO_CSV = os.path.join(DATA_PATH, 'floodtenders_blockgeotagged.csv')
# VARIABLES_ROOT = os.path.join(DATA_PATH, 'variables')

# # ensure root variables dir exists
# os.makedirs(VARIABLES_ROOT, exist_ok=True)

# # load GeoJSON and tagged tenders
# od_gdf = gpd.read_file(SHAPEFILE)
# flood_df = pd.read_csv(GEO_CSV)

# # merge geometry
# flood_df = flood_df.merge(
#     od_gdf[['dtname','object_id','geometry']],
#     left_on=['DISTRICT_FINALISED', 'BLOCK_FINALISED'],
#     right_on=['dtname', 'sdtname'],
#     how='left'
# )
# print(f'flood merged df is: {flood_df}')

# # normalize Awarded Value
# flood_df.rename(columns={'Awarded Price in ₹': 'Awarded Value'}, inplace=True)
# flood_df['Awarded Value'] = flood_df['Awarded Value'].astype(float)

# # 1) Total tender awarded value by month & block
# total_var = 'total_tender_awarded_value'
# tot = (
#     flood_df
#     .groupby(['month', 'object_id'])['Awarded Value']
#     .sum()
#     .reset_index(name=total_var)
# )

# for ym, grp in tot.groupby('month'):
#     out_dir = os.path.join(VARIABLES_ROOT, total_var)
#     os.makedirs(out_dir, exist_ok=True)
#     out_path = os.path.join(out_dir, f'{total_var}_{ym}.csv')
#     grp[['object_id', total_var]].to_csv(out_path, index=False)
#     print(f'Wrote: {out_path}')

# # 2) Scheme-wise tender awarded values
# for scheme in flood_df['Scheme'].dropna().unique():
#     df_s = (
#         flood_df[flood_df['Scheme'] == scheme]
#         .groupby(['month', 'object_id'])['Awarded Value']
#         .sum()
#         .reset_index(name=f'{scheme}_tenders_awarded_value')
#     )
#     var_name = f'{scheme}_tenders_awarded_value'
#     out_dir = os.path.join(VARIABLES_ROOT, var_name)
#     os.makedirs(out_dir, exist_ok=True)
#     for ym, grp in df_s.groupby('month'):
#         out_path = os.path.join(out_dir, f'{var_name}_{ym}.csv')
#         grp[['object_id', var_name]].to_csv(out_path, index=False)
#         print(f'Wrote: {out_path}')

# # 3) Response-type tender awarded values
# for resp in flood_df['Response Type'].dropna().unique():
#     df_r = (
#         flood_df[flood_df['Response Type'] == resp]
#         .groupby(['month', 'object_id'])['Awarded Value']
#         .sum()
#         .reset_index(name=f'{resp}_tenders_awarded_value')
#     )
#     var_name = f'{resp}_tenders_awarded_value'
#     out_dir = os.path.join(VARIABLES_ROOT, var_name)
#     os.makedirs(out_dir, exist_ok=True)
#     for ym, grp in df_r.groupby('month'):
#         out_path = os.path.join(out_dir, f'{var_name}_{ym}.csv')
#         grp[['object_id', var_name]].to_csv(out_path, index=False)
#         print(f'Wrote: {out_path}')
