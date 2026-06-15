import os
import json
import time
import logging
import requests
from pathlib import Path
from shapely.geometry import shape, mapping

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_YEAR = "2019"


class WorldPopDataFetcher:
    def __init__(
        self,
        base_url="https://api.worldpop.org/v1",
        year=DEFAULT_YEAR,
        output_dir=None,
        api_key=None,
        simplify_tolerance=0.01,
        truncate_precision=None,
        async_threshold=1500,
    ):
        self.base_url = base_url.rstrip("/")
        self.year = str(year)
        self.api_key = api_key
        self.simplify_tolerance = simplify_tolerance
        self.truncate_precision = truncate_precision
        self.async_threshold = async_threshold

        if output_dir is not None:
            self.output_dir = Path(output_dir) / self.year
        else:
            self.output_dir = Path(
                "/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar/Sources/WORLDPOP/data/agesexstructure"
            ) / self.year

        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Geometry helpers
    # ------------------------------------------------------------------

    def simplify_geometry(self, geojson, tolerance=None):
        tol = tolerance if tolerance is not None else self.simplify_tolerance

        feature = geojson["features"][0]
        geom = shape(feature["geometry"])

        simplified = geom.simplify(float(tol), preserve_topology=True)
        feature["geometry"] = mapping(simplified)

        return geojson

    def truncate_coordinates(self, geojson, precision=None):
        if precision is None:
            precision = self.truncate_precision

        if precision is None:
            return geojson

        def _trunc(x):
            return round(float(x), precision)

        feature = geojson["features"][0]
        geom_type = feature["geometry"]["type"]
        coords = feature["geometry"]["coordinates"]

        if geom_type == "Polygon":
            feature["geometry"]["coordinates"] = [
                [[_trunc(x) for x in pt] for pt in ring]
                for ring in coords
            ]

        elif geom_type == "MultiPolygon":
            feature["geometry"]["coordinates"] = [
                [
                    [[_trunc(x) for x in pt] for pt in ring]
                    for ring in poly
                ]
                for poly in coords
            ]

        return geojson

    def _prepare_geojson(self, geojson_path):
        with open(geojson_path, "r") as fh:
            gj = json.load(fh)

        if "features" not in gj or len(gj["features"]) == 0:
            raise ValueError(
                "GeoJSON must be a FeatureCollection with at least one feature"
            )

        gj = self.simplify_geometry(
            gj,
            tolerance=self.simplify_tolerance
        )

        if self.truncate_precision is not None:
            gj = self.truncate_coordinates(
                gj,
                precision=self.truncate_precision
            )

        geojson_str = json.dumps(gj, separators=(",", ":"))

        return gj, geojson_str

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    def _build_params(self, dataset, year, geojson_str, runasync):
        params = {
            "dataset": dataset,
            "year": str(year),
            "geojson": geojson_str,
            "runasync": "true" if runasync else "false",
        }

        if self.api_key:
            params["key"] = self.api_key

        return params

    def _poll_task(
        self,
        task_id,
        max_attempts=12,
        initial_delay=1,
        max_delay=30,
    ):
        task_url = f"{self.base_url}/tasks/{task_id}"

        attempt = 0
        delay = initial_delay

        while attempt < max_attempts:

            logger.info(
                f"Polling task {task_id} "
                f"({attempt + 1}/{max_attempts})"
            )

            try:
                response = requests.get(task_url, timeout=30)
                response.raise_for_status()

                data = response.json()

            except requests.RequestException as e:
                logger.warning(
                    f"Polling failed: {e}. "
                    f"Retrying in {delay}s"
                )

                time.sleep(delay)

                delay = min(delay * 2, max_delay)
                attempt += 1

                continue

            status = data.get("status")

            if status == "finished":
                logger.info(f"Task {task_id} finished")
                return data

            if status == "failed":
                logger.error(
                    f"Task {task_id} failed: "
                    f"{data.get('error_message')}"
                )
                return None

            time.sleep(delay)

            delay = min(delay * 2, max_delay)
            attempt += 1

        logger.error("Polling timed out")

        return None

    def _make_api_call(
        self,
        geojson,
        dataset,
        year=None,
        runasync=None,
    ):
        year = str(year or self.year)

        if runasync is None:
            runasync = (
                len(json.dumps(geojson))
                > self.async_threshold
            )

        geojson_str = json.dumps(
            geojson,
            separators=(",", ":")
        )

        params = self._build_params(
            dataset,
            year,
            geojson_str,
            runasync
        )

        stats_url = f"{self.base_url}/services/stats"

        try:
            response = requests.get(
                stats_url,
                params=params,
                timeout=60
            )

            response.raise_for_status()

            data = response.json()

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None

        if "taskid" in data:
            return self._poll_task(data["taskid"])

        if "data" in data:
            return data

        logger.error(f"Unexpected response: {data}")

        return None

    # ------------------------------------------------------------------
    # Save helpers
    # ------------------------------------------------------------------

    def _save_pyramid_data(self, data, district):

        if (
            not data
            or "data" not in data
            or "agesexpyramid" not in data["data"]
        ):
            logger.error(
                f"No agesexpyramid found for {district}"
            )
            return

        outfile = (
            self.output_dir
            / f"{district}_agesexpyramid_{self.year}.csv"
        )

        with open(outfile, "w") as fh:

            fh.write("class,age,male,female\n")

            for row in data["data"]["agesexpyramid"]:

                fh.write(
                    f"{row.get('class','')},"
                    f"{row.get('age','')},"
                    f"{row.get('male','')},"
                    f"{row.get('female','')}\n"
                )

        logger.info(f"Saved {outfile}")

    # ------------------------------------------------------------------
    # Main fetch
    # ------------------------------------------------------------------

    def fetch_worldpop_data(
        self,
        geojson_path,
        dataset="wpgpas",
        year=None,
    ):

        district = Path(geojson_path).stem

        try:

            geojson, geojson_str = self._prepare_geojson(
                geojson_path
            )

            if len(geojson_str) > 8000:

                logger.warning(
                    "Large geometry detected. "
                    "Applying stronger simplification."
                )

                geojson = self.simplify_geometry(
                    geojson,
                    tolerance=self.simplify_tolerance * 10
                )

                geojson = self.truncate_coordinates(
                    geojson,
                    precision=3
                )

            response = self._make_api_call(
                geojson,
                dataset,
                year
            )

            if not response:
                logger.error(
                    f"No response for {district}"
                )
                return False

            self._save_pyramid_data(
                response,
                district
            )

            return True

        except Exception as e:
            logger.exception(
                f"Error processing {district}: {e}"
            )
            return False


# ----------------------------------------------------------------------
# Batch Runner
# ----------------------------------------------------------------------

def main():

    YEAR = "2017"

    fetcher = WorldPopDataFetcher(
        year=YEAR,
        simplify_tolerance=0.01,
        truncate_precision=3,
        async_threshold=1500,
        output_dir="/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar/Sources/WORLDPOP/data/agesexstructure"
    )

    geojson_dir = Path(
        "/Users/stephensmathew/cdl_rep/flood-data-ecosystem-Bihar/Sources/WORLDPOP/scraper_data/shapefiles/district_geojson"
    )

    files = sorted(geojson_dir.glob("*.geojson"))

    logger.info(
        f"Found {len(files)} geojson files"
    )

    for geojson_file in files:

        district = geojson_file.stem

        output_csv = (
            fetcher.output_dir
            / f"{district}_agesexpyramid_{YEAR}.csv"
        )

        if output_csv.exists():
            logger.info(
                f"Skipping {district} "
                f"(already downloaded)"
            )
            continue

        logger.info(
            f"Processing {district}"
        )

        success = fetcher.fetch_worldpop_data(
            str(geojson_file),
            dataset="wpgpas",
            year=YEAR
        )

        if not success:
            logger.error(
                f"Failed for {district}"
            )


if __name__ == "__main__":
    main()