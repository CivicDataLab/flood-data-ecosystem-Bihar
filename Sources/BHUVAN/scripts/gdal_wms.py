import os
import subprocess
import timeit

from osgeo import gdal

gdal.DontUseExceptions()

path = os.getcwd() + "/Sources/BHUVAN/"

date_strings = [
    '''    #2024
    "2024_02_07_06",
    "2024_17_07_06",
    "2024_31_08_18",
    "2024_12_07_06",
    "2024_22_07_18",
    "2024_02_07_18",
    "2024_27_07_06",
    "2024_13_08_06",
    "2024_24_08_10",
    "2024_19_07_10",
    "2024_04_08_06",
    "2024_07_08_18",
    "2024_08_08_18",
    "2024_24_07_06",
    "2024_24_08_18",
    "2024_05_09_10",
    "2024_08_09",
    "2024_09_07_06",
    "2024_10_07_06",
    "2024_14_07_18",
    "2024_14_08_18",
    "2024_17_08_18",
    "2024_25_08_18",
    "2024_28_08_10",
    "2024_02_09_10",
    "2024_10_08_06",
    "2024_19_08_18",
    "2024_03_09_06",
    "2024_10_09_18",
    "2024_30_07_18",
    "2024_15_08_06",
    "2024_21_08_06",
    "2024_07_09_18",
    "2024_16_08_18",
    "2024_12_09_10",
    "2024_15_09_06",
    "2024_16_09_06",
    "2024_25_09_18",
    "2024_27_09_06",
    "2024_01_10_10",
    "2024_01_10_18",
    "2024_02_10_06",
    "2024_04_10_06",
    "2024_03_10_18",
    "2024_06_10_18",
    "2024_09_10_10",
    "2024_11_10_06",
    #"2024_13_10_18",
    '''
    #2023
    "2023_10_07_18",
    "2023_11_08_18",
    "2023_16_08_06",
    "2023_20_08_18",
    "2023_22_08_18",
    "2023_23_08_06",
    "2023_28_08_06",
    "2023_30_08_06",
    "2023_01_09_18",
    "2023_04_09_06"

    #2022

    "2022_04_07_06",
    "2022_25_07",
    "2022_01_02_08",
    "2022_04_08_06",
    "2022_06_08_18",
    "2022_09_08_06",
    "2022_13_08_06",
    "2022_21_08",
    "2022_25_26_08",
    "2022_29_08_06",
    "2022_30_08_06",
    "2022_31_08_18",
    "2022_02_09_06",
    "2022_04_05_09",
    "2022_06_09_18",
    "2022_07_09_18",
    "2022_11_09_18",
    "2022_14_09_06",
    "2022_16_09_06",
    "2022_16_09_18",
    "2022_18_09_18",
    "2022_19_09_18",
    "2022_21_09",
    "2022_23_24_09",
    "2022_26_09_06",
    "2022_27_09",
    "2022_05_10_18",
    "2022_08_10_06",
    "2022_10_11_10",
    "2022_15_10_10",
    "2022_15_10_18",
    "2022_20_10_06"

    ]  # Sample date for assam - "2023_07_07_18"

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
