import os
import sys
import subprocess
from datetime import datetime


os.environ["PROJ_LIB"] = r"C:\Program Files\QGIS 3.42.3\share\proj"
os.environ["GDAL_DATA"] = r"C:\Program Files\QGIS 3.42.3\share\gdal"
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_PATH, "data")
TIFF_DATA_FOLDER = os.path.join(DATA_PATH, "rain", "tiff")
CSV_DATA_FOLDER = os.path.join(DATA_PATH, "rain", "csv")
GDALWARP_PATH = r"C:\Program Files\QGIS 3.42.3\bin\gdalwarp.exe"
GDALCALC_PATH = r"C:\Program Files\QGIS 3.42.3\apps\Python312\Scripts\gdal_calc.py"
PYTHON_QGIS = r"C:\Program Files\QGIS 3.42.3\bin\python-qgis.bat"

INPUT_VECTOR_FILE = (
    r"D:\CDL\flood-data-ecosystem-Bihar\Maps\br-ids-drr_shapefile"
    r"\Bihar_Subdistrict_final_simplified.geojson"
)


#from osgeo.gdal import deprecation_warn
import geopandas as gpd 
import imdlib as imd 
import numpy as np 
import pandas as pd 
import rasterio
import rasterstats
from rasterio.crs import CRS
from rasterio.transform import Affine
import rioxarray



def run(cmd_list: list[str]):
    """Run an external command, printing it first and aborting on failure."""
    print("▶", " ".join(cmd_list))
    subprocess.run(cmd_list, check=True)



def download_data(year: int, start_date: str, end_date: str):
    """Download IMD rainfall for the given *year* into DATA_PATH."""
    current_year = datetime.now().year
    if year == current_year:
        imd.get_real_data(
            var_type="rain",
            start_dy=start_date,
            end_dy=end_date,
            file_dir=DATA_PATH,
        )
    else:
        imd.get_data(
            var_type="rain",
            start_yr=year,
            end_yr=year,
            fn_format="yearwise",
            file_dir=DATA_PATH,
        )


def transform_resample_monthly_tif_filenames(tif_filename: str):
    """Re‑project + resample an IMD monthly TIFF and normalise values."""


    new_transform = Affine(0.25, 0.0, 66.375, 0.0, -0.25, 38.625)

    with rasterio.open(tif_filename, "r+") as raster:
        raster.crs = CRS.from_epsg(4326)
        raster_array = raster.read(1)
        raster_array[np.isnan(raster_array)] = -999
        raster.nodata = -999
        raster.transform = new_transform
        meta = raster.meta

    meta["transform"] = new_transform

    flipped_file = tif_filename.replace(".tif", "_flipped.tif")
    with rasterio.open(flipped_file, "w", **meta) as dst:
        dst.write(raster_array, 1)


    resampled_file = tif_filename.replace(".tif", "_resampled.tif")
    run([
        GDALWARP_PATH,
        "-overwrite", "-tr", "0.01", "-0.01", "-r", "sum",
        "-srcnodata", "-999", "-dstnodata", "-999",
        flipped_file,
        resampled_file,
        "-co", "COMPRESS=DEFLATE",
    ])

    resampled2_file = tif_filename.replace(".tif", "_resampled2.tif")
    run([
        PYTHON_QGIS, GDALCALC_PATH,
        "-A", resampled_file,
        f"--outfile={resampled2_file}",
        "--calc=A/625",
        "--NoDataValue=-999",
        "--creation-option=COMPRESS=DEFLATE",
        "--overwrite",
    ])


def parse_and_format_data(year: int, start_date: str, end_date: str):
    current_year = datetime.now().year
    if year == current_year:
        data = imd.open_real_data(
            var_type="rain",
            start_dy=start_date,
            end_dy=end_date,
            file_dir=DATA_PATH,
        )
    else:
        data = imd.open_data(
            var_type="rain",
            start_yr=year,
            end_yr=year,
            fn_format="yearwise",
            file_dir=DATA_PATH,
        )

    dataset = data.get_xarray().where(lambda ds: ds["rain"] != -999.0)
    monthly = dataset.groupby("time.month")

    os.makedirs(TIFF_DATA_FOLDER, exist_ok=True)

    for _, ds in monthly:
        month = int(ds["time.month"].values[0])
        tif_name = os.path.join(TIFF_DATA_FOLDER, f"{year}_{month:02d}.tif")
        ds["rain"].sum("time").rio.to_raster(tif_name)
        transform_resample_monthly_tif_filenames(tif_name)

    # Also save year‑wide file to capture CRS
    data.to_geotiff(f"{year}.tif", TIFF_DATA_FOLDER)



def retrieve_up_subdistrict_data(year: int):
    print(f"\n🟢  Starting zonal rainfall processing for {year}")

    gdf = (
        gpd.read_file(INPUT_VECTOR_FILE)
        .set_crs(epsg=3857, allow_override=True)
        .to_crs(epsg=4326)
    )
    print("   • Vector CRS →", gdf.crs)
    print("   • Bounds     →", gdf.total_bounds)
    processed_dir = os.path.join(DATA_PATH, "rain", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    for month in range(1, 13):
        month_str = f"{month:02d}"
        raster_path = os.path.join(TIFF_DATA_FOLDER, f"{year}_{month_str}_resampled2.tif")
        if not os.path.exists(raster_path):
            print(f"   ⏭️  {year}_{month_str}: raster not found")
            continue

        print(f"Processing {year}_{month_str} …")

        with rasterio.open(raster_path) as ras:
            zs = rasterstats.zonal_stats(
                gdf, ras.read(1), affine=ras.transform,
                stats=["count", "mean", "sum", "max"],
                nodata=ras.nodata, geojson_out=True,
            )
        df = pd.concat(pd.DataFrame([f["properties"]]) for f in zs)
        out_csv = os.path.join(processed_dir, f"up_subdistrict_rainfall_{year}_{month_str}.csv")
        df.to_csv(out_csv, index=False)
        print(f"      ✔ Saved {out_csv}")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python main.py <year>")

    YEAR = int(sys.argv[1])
    START_DATE = "2025-01-01"  
    END_DATE = "2025-05-31"    

    download_data(YEAR, start_date=START_DATE, end_date=END_DATE)
    parse_and_format_data(YEAR, start_date=START_DATE, end_date=END_DATE)
    retrieve_up_subdistrict_data(YEAR)
