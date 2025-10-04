import pandas as pd
import geopandas as gpd
import os

#loading the path
repo_root = "/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar"
health_path = os.path.join(repo_root,"Sources/BHARATMAPS/data/Raw data/healthcare_bihar.geojson")
sub_path = os.path.join(repo_root,"Maps/br-ids-drr_shapefile/Bihar_subdistrict_final_4326.geojson")

#fix multilayer

health_gdf = gpd.read_file(health_path).explode(index_parts=True).reset_index(drop =True)
sub_gdf = gpd.read_file(sub_path).explode(index_parts=True).reset_index(drop=True)

#setting the crs right 

health_gdf = health_gdf.to_crs(sub_gdf.crs)

#spatial join

health_in_bihar = gpd.sjoin(health_gdf,sub_gdf, how="left", predicate="within")

# Count health centres per subdistrict
health_centres_count = (
    health_in_bihar.groupby("object_id")
    .size()
    .reset_index(name="health_centres_count")
)

# Save output
output_path = os.path.join(repo_root,"Sources/BHARATMAPS/data/variables/HealthCentres/health_count.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
health_centres_count.to_csv(output_path, index=False)

print("✅ Health count per subdistrict saved to:", output_path)
