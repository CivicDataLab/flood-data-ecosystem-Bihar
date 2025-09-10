import pandas as pd
import geopandas as gpd
import os

# Set repo root
repo_root = "/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar"

# File paths
road_path = os.path.join(repo_root, "Sources/BHARATMAPS/data/Raw data/roads_merged_bihar_length.geojson")
sub_path = os.path.join(repo_root, "Maps/br-ids-drr_shapefile/Bihar_subdistrict_final_4326.geojson")

# Load shapefiles and fix multipart geometries
road_gdf = gpd.read_file(road_path).explode(index_parts=True).reset_index(drop=True)
sub_gdf = gpd.read_file(sub_path).explode(index_parts=True).reset_index(drop=True)

#  Reproject roads to UTM (zone covering Bihar) for length in meters
road_gdf = road_gdf.to_crs(epsg=32645)
road_gdf["LENGTH"] = road_gdf.geometry.length  # length in meters

# Reproject back to WGS84 to align with subdistricts
road_gdf = road_gdf.to_crs(sub_gdf.crs)

# Spatial join using intersects
roads_in_bihar = gpd.sjoin(road_gdf, sub_gdf, how="left", predicate="intersects")

#Detect the subdistrict ID column automatically
id_col = None
for col in roads_in_bihar.columns:
    if "object_id" in col.lower():
        id_col = col
        break

if id_col is None:
    raise ValueError("❌ Could not find an object_id column in joined data. Check subdistrict shapefile schema.")

# Group by detected subdistrict ID and sum road lengths
road_lengths_in_bihar = (
    roads_in_bihar.groupby(id_col)["LENGTH"]
    .sum()
    .reset_index()
    .rename(columns={id_col: "object_id", "LENGTH": "total_road_length_m"})
)

# Save results
output_path = os.path.join(repo_root, "Sources/BHARATMAPS/data/variables/RoadLengths/RoadLengths.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
road_lengths_in_bihar.to_csv(output_path, index=False)

print(f"✅ Total road lengths per subdistrict saved to: {output_path}")
