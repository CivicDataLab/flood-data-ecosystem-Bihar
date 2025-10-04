import pandas as pd
import geopandas as gpd
import os 

repo_root = "/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar"

rail_path = os.path.join(repo_root, "Sources/BHARATMAPS/data/Raw data/rail_length_bihar.geojson")
sub_path = os.path.join(repo_root, "Maps/br-ids-drr_shapefile/Bihar_subdistrict_final_4326.geojson")

# Fix multilayering
rail_gdf = gpd.read_file(rail_path).explode(index_parts=True).reset_index(drop=True)
sub_gdf = gpd.read_file(sub_path).explode(index_parts=True).reset_index(drop=True)

# Align CRS 
rail_gdf = rail_gdf.to_crs(sub_gdf.crs)

# Spatial join using intersects
rails_in_bihar = gpd.sjoin(rail_gdf, sub_gdf, how="left", predicate="intersects")

# Detect the correct object_id column from subdistrict shapefile
id_col = None
for col in rails_in_bihar.columns:
    if "object_id" in col.lower():
        id_col = col
        break

if id_col is None:
    raise ValueError("Could not find an object_id column in joined data. Check subdistrict shapefile schema.")

# Group by subdistrict and calculate total rail length and count (from file)
rail_lengths_bihar = (
    rails_in_bihar.groupby(id_col).agg(
        rail_length=("LENGTH", "sum"),  # sum of rail lengths
        rail_count=("COUNT", "sum")           # sum of COUNT field
    )
    .reset_index()
    .rename(columns={id_col: "object_id"})
)

# Save output
output_path = os.path.join(repo_root, "Sources/BHARATMAPS/data/variables/RailLengths/RailLengths.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
rail_lengths_bihar.to_csv(output_path, index=False)

print(f"✅ Total rail lengths and segment counts per subdistrict saved to: {output_path}")
