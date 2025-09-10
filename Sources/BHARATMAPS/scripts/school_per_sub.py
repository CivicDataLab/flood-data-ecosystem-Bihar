import pandas as pd
import geopandas as gpd
import os

#loading the paths 
repo_root = "/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar"
school_path = os.path.join(repo_root,"Sources/BHARATMAPS/data/Raw data/school_bihar.geojson")
sub_path = os.path.join(repo_root,"Maps/br-ids-drr_shapefile/Bihar_subdistrict_final_4326.geojson")

# fixing the multilayer 

school_gdf = gpd.read_file(school_path).explode(index_parts=True).reset_index(drop = True)
sub_gdf = gpd.read_file(sub_path).explode(index_parts=True).reset_index(drop = True)

#crs matching 

school_gdf = school_gdf.to_crs(sub_gdf.crs)

#path join

school_in_bihar = gpd.sjoin(school_gdf,sub_gdf, how="left", predicate="within")

school_count = (
    school_in_bihar.groupby("object_id")
    .size()
    .reset_index(name="school_count")
)

# Save output
output_path = os.path.join(repo_root,"Sources/BHARATMAPS/data/variables/schools/school_count.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
school_count.to_csv(output_path, index=False)

print("✅ school count per subdistrict saved to:", output_path)


