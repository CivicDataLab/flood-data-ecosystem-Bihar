import os
import subprocess
import timeit

from osgeo import gdal

gdal.DontUseExceptions()

path = os.getcwd() + "/Sources/BHUVAN/"

date_strings = [
    "2025_07_09_18",
    "2025_10_09_06",
    "2025_11_09_18",
    "2025_14_09_18",
    "2025_16_09_18",
    "2025_17_09_18",
    "2025_05_09_06",
    "2025_08_09_06",
    "2025_12_09_18",
    "2025_15_09_06",
    "2025_07_09_10",
    "2025_18_07_11",
    "2025_19_07_18",
    "2025_19_07_06",
    "2025_21_07_18",
    "2025_22_07_18",
    "2025_23_07_10",
    "2025_23_07_18",
    "2025_23_07",
    "2025_24_07_06",
    "2025_27_07_18",
    "2025_28_07_18",
    "2025_31_07_06",
    "2025_31_07_18",
    "2025_02_08_18",
    "2025_05_08_06",
    "2025_05_08_18",
    "2025_07_08_10",
    "2025_07_08_18",
    "2025_09_08_10",
    "2025_09_08_18",
    "2025_12_08_18",
    "2025_13_08_18",
    "2025_14_08_06",
    "2025_14_08_18",
    "2025_15_08_10",
    "2025_17_08_06",
    "2025_18_08_06",
    "2025_19_08_18",
    "2025_21_08_06",
    "2025_22_08_06",
    "2025_24_08_06",
    "2025_25_08_18",
    "2025_29_08_06",
    "2025_30_08_06",
    "2025_31_08_18",
    "2025_01_09_10",
    "2025_01_09",
    "2025_02_09_18",
    "2025_03_09_18",
    "2025_03_09_06",
    "2025_10_09_18",
    "2025_22_09_06",
    "2025_24_09_06",
    "2025_25_09_06",
    "2025_26_09_10",
    "2025_26_09_18",
    "2025_28_09_18",
    "2025_29_09_18",
    "2025_03_10_06",
    "2025_03_10_18",
    "2025_07_10_18",
    "2025_08_10_10",
    "2025_09_10_06",
    "2025_10_10_10",
    "2025_11_10_06",
    "2025_12_10_06",
    "2025_12_10_18",
    "2025_13_10_10",
    "2025_13_10_18",
    "2025_16_10_18",
    "2025_16_10_06",
    "2025_20_10_10",
    "2025_21_10_06"
]  

# Sample date for assam - "2023_07_07_18"

# Specify the state information to scrape data for.
# state_info = {"state": "Assam", "code": "as"}


for dates in date_strings:

    # Define your input and output paths
    input_xml_path = path + "/data/inundation.xml"
    output_tiff_path = path + f"/data/tiffs/{dates}.tif"

    layer_up = "flood%3Abr"
    bbox_up =  "83.31, 24.28, 88.30, 27.86"  #"77.08,23.87,84.63,30.40" #"89.6922970,23.990548,96.0205936,28.1690311"

    url_cached = "https://bhuvan-ras2.nrsc.gov.in/mapcache"
    url_up = "https://bhuvan-gp1.nrsc.gov.in/bhuvan/wms"

    # Download the WMS(Web Map Sevice) layer and save as XML.
    command = [
        "gdal_translate",
        "-of",
        "WMS",
        f"WMS:{url_up}?&LAYERS={layer_up}_{dates}&TRANSPARENT=TRUE&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=&FORMAT=image%2Fpng&SRS=EPSG%3A4326&BBOX={bbox_up}",
        f"{path}/data/inundation.xml",
    ]
    subprocess.run(command)

    # Specify the target resolution in the X and Y directions (50 meters)
    target_resolution_x = 0.00044915  # 0.0008983  # 0.0001716660336923202072
    target_resolution_y = -0.00044915  # -0.0008983  # -0.0001716684356881450775

    # Perform the warp operation using gdal.Warp()
    print("Warping Started")
    starttime = timeit.default_timer()

    gdal.Warp(
        output_tiff_path,
        input_xml_path,
        format="GTiff",
        xRes=target_resolution_x,
        yRes=target_resolution_y,
        creationOptions=["COMPRESS=DEFLATE", "TILED=YES"],
        callback=gdal.TermProgress,
    )

    print("Time took to Warp: ", timeit.default_timer() - starttime)
    print(f"Warping completed. Output saved to: {output_tiff_path}")
