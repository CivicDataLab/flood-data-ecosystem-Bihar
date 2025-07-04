import pandas as pd
import os
from rapidfuzz import process, fuzz
import geopandas as gpd

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
    match, score, _ = process.extractOne(query, choices, scorer=scorer)
    return match if score >= score_cutoff else None


# data_path = os.getcwd()+'/Sources/TENDERS/data/'
data_path='/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Sources/TENDERS/data/'
od_gdf = gpd.read_file('/home/prajna/civicdatalab/ids-drr/bihar/flood-data-ecosystem-Bihar/Maps/br-ids-drr_shapefile/Bihar_Subdistrict_final_simplified.geojson')

flood_tenders_geotagged_df = pd.read_csv(data_path + '/floodtenders_blockgeotagged.csv')

# FUzzy match
sdt_choices=od_gdf['sdtname'].dropna().unique().tolist()

# --- compute a fuzzy‐matched sdtname column ---
flood_tenders_geotagged_df['matched_sdtname'] = (
    flood_tenders_geotagged_df['BLOCK_FINALISED']
    .astype(str)
    .str.title()  # normalize casing
    .apply(lambda blk: best_match(blk, sdt_choices))
)


flood_tenders_geotagged_df = flood_tenders_geotagged_df.merge( od_gdf[['dtname', 'sdtname', 'object_id', 'geometry']],
    left_on = ['DISTRICT_FINALISED', 'matched_sdtname'],
    right_on = ['dtname', 'sdtname'],
    how     = 'left')
print(flood_tenders_geotagged_df)
flood_tenders_geotagged_df.rename(columns={'Awarded Price in ₹':'Awarded Value'},inplace = True)
flood_tenders_geotagged_df['Awarded Value'] = (flood_tenders_geotagged_df['Awarded Value'].astype(float))

# Total tender variable
variable = 'total_tender_awarded_value'
total_tender_awarded_value_df = flood_tenders_geotagged_df.groupby(['month', 'object_id'])[['Awarded Value']].sum().reset_index()
total_tender_awarded_value_df = total_tender_awarded_value_df.rename(columns = {'Awarded Value': variable})

for year_month in total_tender_awarded_value_df.month.unique():
    variable_df_monthly = total_tender_awarded_value_df[total_tender_awarded_value_df.month == year_month]
    variable_df_monthly = variable_df_monthly[['object_id', variable]]
    if os.path.exists(data_path+'variables/'+variable):
        print(f"data path is {data_path}")
        variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)
    else:
        os.mkdir(data_path+'variables/'+variable)
        print(f"data path is {data_path}")
        variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)

# Scheme wise tender variables
variables = flood_tenders_geotagged_df['Scheme'].unique()
for variable in variables:
    variable_df = flood_tenders_geotagged_df[flood_tenders_geotagged_df['Scheme'] == variable]
    variable_df= variable_df.groupby(['month', 'object_id'])[['Awarded Value']].sum().reset_index()
    
    variable = str(variable) + '_tenders_awarded_value'
    variable_df = variable_df.rename(columns = {'Awarded Value': variable})
    
    for year_month in variable_df.month.unique():
        variable_df_monthly = variable_df[variable_df.month == year_month]
        variable_df_monthly = variable_df_monthly[['object_id', variable]]
        if os.path.exists(data_path+'variables/'+variable):
            variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)
        else:
            os.mkdir(data_path+'variables/'+variable)
            variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)


# Scheme wise tender variables
variables = flood_tenders_geotagged_df['Response Type'].unique()
for variable in variables:
    variable_df = flood_tenders_geotagged_df[flood_tenders_geotagged_df['Response Type'] == variable]
    variable_df= variable_df.groupby(['month', 'object_id'])[['Awarded Value']].sum().reset_index()

    variable = str(variable) + '_tenders_awarded_value'
    variable_df = variable_df.rename(columns = {'Awarded Value': variable})

    for year_month in variable_df.month.unique():
        variable_df_monthly = variable_df[variable_df.month == year_month]
        variable_df_monthly = variable_df_monthly[['object_id', variable]]
        if os.path.exists(data_path+'variables/'+variable):
            print(f'The variables is: ', variable_df_monthly)
            variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)
        else:
            os.mkdir(data_path+'variables/'+variable)
            print(f'The variables is: ', variable_df_monthly)
            variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)

    '''
    for year_month in variable_df.month.unique():
        variable_df_monthly = variable_df[variable_df.month == year_month]
        variable_df_monthly = variable_df_monthly[['object_id_id', variable]]
        if os.path.exists(data_path+'variables/'+variable):
            variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)
        else:
            os.mkdir(data_path+'variables/'+variable)
            variable_df_monthly.to_csv(data_path+'variables/'+variable+'/{}_{}.csv'.format(variable, year_month), index=False)
'''

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
