#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""
Milvus GIS query implementations for SpatialBench.

Supported queries: Q1, Q2, Q3, Q4, Q6, Q8, Q9, Q10, Q11
Unsupported queries: Q5 (ST_ConvexHull, ST_Collect, ST_Area),
                     Q7 (ST_MakeLine, ST_Length),
                     Q12 (ST_KNN)

Note: This module requires data to be loaded into Milvus first using milvus_data_loader.py
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame
from pymilvus import MilvusClient
from shapely import wkb
from shapely.geometry import MultiPoint, Point, Polygon

# Milvus collection names (with configurable prefix)
COLLECTION_PREFIX = "spatialbench"

# Supported queries list
SUPPORTED_QUERIES = ["q1", "q2", "q3", "q4", "q6", "q8", "q9", "q10", "q11"]
UNSUPPORTED_QUERIES = ["q5", "q7", "q12"]


class MilvusQueryRunner:
    """Runner for Milvus GIS queries."""

    def __init__(self, uri: str = "http://localhost:19530", prefix: str = COLLECTION_PREFIX):
        self.uri = uri
        self.prefix = prefix
        self.client: MilvusClient | None = None

    def connect(self) -> None:
        """Connect to Milvus server."""
        self.client = MilvusClient(uri=self.uri)

    def disconnect(self) -> None:
        """Disconnect from Milvus server."""
        if self.client:
            self.client.close()
            self.client = None

    def _collection_name(self, table: str) -> str:
        """Get full collection name with prefix."""
        return f"{self.prefix}_{table}"

    def _query(self, collection: str, filter_expr: str, output_fields: list[str], limit: int = 16384) -> list[dict]:
        """Execute a query on a collection."""
        return self.client.query(
            collection_name=self._collection_name(collection),
            filter=filter_expr,
            output_fields=output_fields,
            limit=limit,
        )


def _get_runner(data_paths: dict[str, str]) -> MilvusQueryRunner:
    """Get a connected Milvus query runner.

    data_paths is not used directly since Milvus queries use pre-loaded collections,
    but we keep the signature for consistency with other query implementations.
    """
    # Get Milvus connection info from environment or use defaults
    import os
    uri = os.environ.get("MILVUS_URI", "http://localhost:19530")
    prefix = os.environ.get("MILVUS_PREFIX", COLLECTION_PREFIX)

    runner = MilvusQueryRunner(uri=uri, prefix=prefix)
    runner.connect()
    return runner


def q1(data_paths: dict[str, str]) -> DataFrame:
    """Q1 (Milvus): Trips starting within 50km of Sedona city center.

    Uses ST_DWithin for spatial filtering.
    """
    runner = _get_runner(data_paths)
    try:
        # Sedona city center coordinates and radius (0.45 degrees ~ 50km)
        center_wkt = "POINT(-111.7610 34.8697)"
        radius = 0.45

        # Query using Milvus GIS filter
        filter_expr = f"GEOM_DWITHIN(t_pickuploc, '{center_wkt}', {radius})"
        results = runner._query(
            "trip",
            filter_expr,
            ["t_tripkey", "t_pickuploc", "t_pickuptime"],
            limit=1000000,
        )

        if not results:
            return pd.DataFrame(columns=["t_tripkey", "pickup_lon", "pickup_lat", "t_pickuptime", "distance_to_center"])

        # Convert to DataFrame and compute distances
        df = pd.DataFrame(results)
        center = Point(-111.7610, 34.8697)

        # Parse WKT geometry and extract coordinates
        from shapely import wkt as shapely_wkt
        df["pickup_geom"] = df["t_pickuploc"].apply(shapely_wkt.loads)
        df["pickup_lon"] = df["pickup_geom"].apply(lambda g: g.x)
        df["pickup_lat"] = df["pickup_geom"].apply(lambda g: g.y)
        df["distance_to_center"] = df["pickup_geom"].apply(lambda g: g.distance(center))

        # Sort and select columns
        result = df.sort_values(
            ["distance_to_center", "t_tripkey"], ascending=[True, True]
        )[["t_tripkey", "pickup_lon", "pickup_lat", "t_pickuptime", "distance_to_center"]].reset_index(drop=True)

        return result
    finally:
        runner.disconnect()


def q2(data_paths: dict[str, str]) -> DataFrame:
    """Q2 (Milvus): Count trips starting within Coconino County zone.

    Uses ST_Intersects for point-in-polygon test.
    """
    runner = _get_runner(data_paths)
    try:
        # First, get Coconino County boundary
        zone_results = runner._query(
            "zone",
            "z_name == 'Coconino County'",
            ["z_boundary"],
            limit=1,
        )

        if not zone_results:
            return pd.DataFrame({"trip_count_in_coconino_county": [0]})

        county_wkt = zone_results[0]["z_boundary"]

        # Count trips intersecting the county
        filter_expr = f"GEOM_INTERSECTS(t_pickuploc, '{county_wkt}')"
        results = runner._query(
            "trip",
            filter_expr,
            ["t_tripkey"],
            limit=10000000,
        )

        count = len(results)
        return pd.DataFrame({"trip_count_in_coconino_county": [count]})
    finally:
        runner.disconnect()


def q3(data_paths: dict[str, str]) -> DataFrame:
    """Q3 (Milvus): Monthly trip statistics within 15km radius of Sedona city center.

    Uses ST_DWithin with polygon + buffer.
    """
    runner = _get_runner(data_paths)
    try:
        # 10km bounding box around Sedona + 5km buffer
        box_wkt = "POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))"
        buffer_distance = 0.045  # ~5km

        filter_expr = f"GEOM_DWITHIN(t_pickuploc, '{box_wkt}', {buffer_distance})"
        results = runner._query(
            "trip",
            filter_expr,
            ["t_tripkey", "t_pickuptime", "t_dropofftime", "t_distance", "t_fare"],
            limit=10000000,
        )

        if not results:
            return pd.DataFrame(columns=["pickup_month", "total_trips", "avg_distance", "avg_duration", "avg_fare"])

        df = pd.DataFrame(results)

        # Convert timestamps
        df["t_pickuptime"] = pd.to_datetime(df["t_pickuptime"])
        df["t_dropofftime"] = pd.to_datetime(df["t_dropofftime"])

        # Compute duration in seconds
        df["_duration_seconds"] = (df["t_dropofftime"] - df["t_pickuptime"]).dt.total_seconds()

        # Group by month
        df["pickup_month"] = df["t_pickuptime"].dt.to_period("M").dt.to_timestamp()

        agg = (
            df.groupby("pickup_month", as_index=False)
            .agg(
                total_trips=("t_tripkey", "count"),
                avg_distance=("t_distance", "mean"),
                avg_duration=("_duration_seconds", "mean"),
                avg_fare=("t_fare", "mean"),
            )
            .sort_values("pickup_month")
            .reset_index(drop=True)
        )

        return agg
    finally:
        runner.disconnect()


def q4(data_paths: dict[str, str]) -> DataFrame:
    """Q4 (Milvus): Zone distribution of top 1000 trips by tip amount.

    Uses ST_Within for spatial join (implemented via client-side processing).
    """
    runner = _get_runner(data_paths)
    try:
        # Get top 1000 trips by tip
        trip_results = runner._query(
            "trip",
            "",  # No filter, get all and sort client-side
            ["t_tripkey", "t_pickuploc", "t_tip"],
            limit=10000000,
        )

        if not trip_results:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "trip_count"])

        trip_df = pd.DataFrame(trip_results)
        top_trips = trip_df.sort_values(
            ["t_tip", "t_tripkey"], ascending=[False, True]
        ).head(1000)

        # Get all zones
        zone_results = runner._query(
            "zone",
            "",
            ["z_zonekey", "z_name", "z_boundary"],
            limit=100000,
        )

        if not zone_results:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "trip_count"])

        zone_df = pd.DataFrame(zone_results)

        # Parse geometries
        from shapely import wkt as shapely_wkt
        top_trips["pickup_geom"] = top_trips["t_pickuploc"].apply(shapely_wkt.loads)
        zone_df["zone_geom"] = zone_df["z_boundary"].apply(shapely_wkt.loads)

        # Perform spatial join (point within polygon)
        results = []
        for _, zone_row in zone_df.iterrows():
            zone_geom = zone_row["zone_geom"]
            count = sum(1 for pt in top_trips["pickup_geom"] if zone_geom.contains(pt))
            if count > 0:
                results.append({
                    "z_zonekey": zone_row["z_zonekey"],
                    "z_name": zone_row["z_name"],
                    "trip_count": count,
                })

        result_df = pd.DataFrame(results)
        if result_df.empty:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "trip_count"])

        return result_df.sort_values(
            ["trip_count", "z_zonekey"], ascending=[False, True]
        ).reset_index(drop=True)
    finally:
        runner.disconnect()


def q5(data_paths: dict[str, str]) -> DataFrame:
    """Q5 (Milvus): NOT SUPPORTED - requires ST_ConvexHull, ST_Collect, ST_Area.

    These functions are not available in Milvus GIS.
    """
    raise NotImplementedError(
        "Q5 is not supported by Milvus GIS. "
        "Required functions ST_ConvexHull, ST_Collect, ST_Area are not available."
    )


def q6(data_paths: dict[str, str]) -> DataFrame:
    """Q6 (Milvus): Zone statistics for trips intersecting a bounding box.

    Uses ST_Intersects and ST_Within.
    """
    runner = _get_runner(data_paths)
    try:
        # Bounding box
        bbox_wkt = "POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))"

        # Get zones intersecting the bounding box
        filter_expr = f"GEOM_INTERSECTS(z_boundary, '{bbox_wkt}')"
        zone_results = runner._query(
            "zone",
            filter_expr,
            ["z_zonekey", "z_name", "z_boundary"],
            limit=100000,
        )

        if not zone_results:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "total_pickups", "avg_distance", "avg_duration"])

        zone_df = pd.DataFrame(zone_results)

        # Get all trips
        trip_results = runner._query(
            "trip",
            "",
            ["t_tripkey", "t_pickuploc", "t_pickuptime", "t_dropofftime", "t_totalamount", "t_distance"],
            limit=10000000,
        )

        if not trip_results:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "total_pickups", "avg_distance", "avg_duration"])

        trip_df = pd.DataFrame(trip_results)

        # Parse geometries
        from shapely import wkt as shapely_wkt
        trip_df["pickup_geom"] = trip_df["t_pickuploc"].apply(shapely_wkt.loads)
        zone_df["zone_geom"] = zone_df["z_boundary"].apply(shapely_wkt.loads)

        # Convert timestamps
        trip_df["t_pickuptime"] = pd.to_datetime(trip_df["t_pickuptime"])
        trip_df["t_dropofftime"] = pd.to_datetime(trip_df["t_dropofftime"])

        # Perform spatial join and aggregate
        results = []
        for _, zone_row in zone_df.iterrows():
            zone_geom = zone_row["zone_geom"]
            mask = trip_df["pickup_geom"].apply(lambda pt: zone_geom.contains(pt))
            matching_trips = trip_df[mask]

            if len(matching_trips) > 0:
                # Determine distance column
                dist_col = "t_totalamount" if "t_totalamount" in matching_trips.columns else "t_distance"
                avg_dist = matching_trips[dist_col].mean() if dist_col in matching_trips.columns else np.nan

                durations = (matching_trips["t_dropofftime"] - matching_trips["t_pickuptime"]).dt.total_seconds()

                results.append({
                    "z_zonekey": zone_row["z_zonekey"],
                    "z_name": zone_row["z_name"],
                    "total_pickups": len(matching_trips),
                    "avg_distance": avg_dist,
                    "avg_duration": durations.mean(),
                })

        result_df = pd.DataFrame(results)
        if result_df.empty:
            return pd.DataFrame(columns=["z_zonekey", "z_name", "total_pickups", "avg_distance", "avg_duration"])

        return result_df.sort_values(
            ["total_pickups", "z_zonekey"], ascending=[False, True]
        ).reset_index(drop=True)
    finally:
        runner.disconnect()


def q7(data_paths: dict[str, str]) -> DataFrame:
    """Q7 (Milvus): NOT SUPPORTED - requires ST_MakeLine, ST_Length.

    These functions are not available in Milvus GIS.
    """
    raise NotImplementedError(
        "Q7 is not supported by Milvus GIS. "
        "Required functions ST_MakeLine, ST_Length are not available."
    )


def q8(data_paths: dict[str, str]) -> DataFrame:
    """Q8 (Milvus): Count nearby pickups for each building within 500m radius.

    Uses ST_DWithin for proximity query.
    """
    runner = _get_runner(data_paths)
    try:
        # Get all buildings
        building_results = runner._query(
            "building",
            "",
            ["b_buildingkey", "b_name", "b_boundary"],
            limit=1000000,
        )

        if not building_results:
            return pd.DataFrame(columns=["b_buildingkey", "b_name", "nearby_pickup_count"])

        building_df = pd.DataFrame(building_results)

        # Get all trips
        trip_results = runner._query(
            "trip",
            "",
            ["t_tripkey", "t_pickuploc"],
            limit=10000000,
        )

        if not trip_results:
            return pd.DataFrame(columns=["b_buildingkey", "b_name", "nearby_pickup_count"])

        trip_df = pd.DataFrame(trip_results)

        # Parse geometries
        from shapely import wkt as shapely_wkt
        trip_df["pickup_geom"] = trip_df["t_pickuploc"].apply(shapely_wkt.loads)
        building_df["boundary_geom"] = building_df["b_boundary"].apply(shapely_wkt.loads)

        # Distance threshold (~500m in degrees)
        threshold = 0.0045

        # Count nearby pickups for each building
        results = []
        for _, building_row in building_df.iterrows():
            boundary_geom = building_row["boundary_geom"]
            count = sum(
                1 for pt in trip_df["pickup_geom"]
                if pt.distance(boundary_geom) <= threshold
            )
            if count > 0:
                results.append({
                    "b_buildingkey": building_row["b_buildingkey"],
                    "b_name": building_row["b_name"],
                    "nearby_pickup_count": count,
                })

        result_df = pd.DataFrame(results)
        if result_df.empty:
            return pd.DataFrame(columns=["b_buildingkey", "b_name", "nearby_pickup_count"])

        return result_df.sort_values(
            ["nearby_pickup_count", "b_buildingkey"], ascending=[False, True]
        ).reset_index(drop=True)
    finally:
        runner.disconnect()


def q9(data_paths: dict[str, str]) -> DataFrame:
    """Q9 (Milvus): Building conflation via IoU detection.

    Uses ST_Intersects + client-side Shapely for IoU calculation.
    Note: Milvus doesn't have ST_Intersection or ST_Area, so IoU is computed client-side.
    """
    runner = _get_runner(data_paths)
    try:
        # Get all buildings
        building_results = runner._query(
            "building",
            "",
            ["b_buildingkey", "b_boundary"],
            limit=1000000,
        )

        if not building_results:
            return pd.DataFrame(columns=["building_1", "building_2", "area1", "area2", "overlap_area", "iou"])

        building_df = pd.DataFrame(building_results)

        # Parse geometries
        from shapely import wkt as shapely_wkt
        building_df["boundary_geom"] = building_df["b_boundary"].apply(shapely_wkt.loads)

        # Find intersecting pairs and compute IoU
        results = []
        buildings = building_df.to_dict("records")
        n = len(buildings)

        for i in range(n):
            for j in range(i + 1, n):
                b1 = buildings[i]
                b2 = buildings[j]
                geom1 = b1["boundary_geom"]
                geom2 = b2["boundary_geom"]

                if geom1.intersects(geom2):
                    area1 = geom1.area
                    area2 = geom2.area
                    intersection = geom1.intersection(geom2)
                    overlap_area = intersection.area
                    union_area = area1 + area2 - overlap_area

                    if union_area > 0:
                        iou = overlap_area / union_area
                    elif overlap_area > 0:
                        iou = 1.0
                    else:
                        iou = 0.0

                    results.append({
                        "building_1": b1["b_buildingkey"],
                        "building_2": b2["b_buildingkey"],
                        "area1": area1,
                        "area2": area2,
                        "overlap_area": overlap_area,
                        "iou": iou,
                    })

        result_df = pd.DataFrame(results)
        if result_df.empty:
            return pd.DataFrame(columns=["building_1", "building_2", "area1", "area2", "overlap_area", "iou"])

        return result_df.sort_values(
            ["iou", "building_1", "building_2"], ascending=[False, True, True]
        ).reset_index(drop=True)
    finally:
        runner.disconnect()


def q10(data_paths: dict[str, str]) -> DataFrame:
    """Q10 (Milvus): Zone stats for trips starting within each zone.

    Uses ST_Within with LEFT JOIN semantics (zones with 0 trips retained).
    """
    runner = _get_runner(data_paths)
    try:
        # Get all zones
        zone_results = runner._query(
            "zone",
            "",
            ["z_zonekey", "z_name", "z_boundary"],
            limit=1000000,
        )

        if not zone_results:
            return pd.DataFrame(columns=["z_zonekey", "pickup_zone", "avg_duration", "avg_distance", "num_trips"])

        zone_df = pd.DataFrame(zone_results)

        # Get all trips
        trip_results = runner._query(
            "trip",
            "",
            ["t_tripkey", "t_pickuploc", "t_pickuptime", "t_dropofftime", "t_distance"],
            limit=10000000,
        )

        # Parse geometries
        from shapely import wkt as shapely_wkt
        zone_df["zone_geom"] = zone_df["z_boundary"].apply(shapely_wkt.loads)

        if not trip_results:
            # Return all zones with 0 trips
            result = zone_df[["z_zonekey", "z_name"]].copy()
            result["pickup_zone"] = result["z_name"]
            result["avg_duration"] = np.nan
            result["avg_distance"] = np.nan
            result["num_trips"] = 0
            return result[["z_zonekey", "pickup_zone", "avg_duration", "avg_distance", "num_trips"]].sort_values(
                ["avg_duration", "z_zonekey"], ascending=[False, True], na_position="last"
            ).reset_index(drop=True)

        trip_df = pd.DataFrame(trip_results)
        trip_df["pickup_geom"] = trip_df["t_pickuploc"].apply(shapely_wkt.loads)
        trip_df["t_pickuptime"] = pd.to_datetime(trip_df["t_pickuptime"])
        trip_df["t_dropofftime"] = pd.to_datetime(trip_df["t_dropofftime"])

        # Compute stats for each zone
        results = []
        for _, zone_row in zone_df.iterrows():
            zone_geom = zone_row["zone_geom"]
            mask = trip_df["pickup_geom"].apply(lambda pt: zone_geom.contains(pt))
            matching_trips = trip_df[mask]

            if len(matching_trips) > 0:
                durations = (matching_trips["t_dropofftime"] - matching_trips["t_pickuptime"]).dt.total_seconds()
                results.append({
                    "z_zonekey": zone_row["z_zonekey"],
                    "pickup_zone": zone_row["z_name"],
                    "avg_duration": durations.mean(),
                    "avg_distance": matching_trips["t_distance"].mean(),
                    "num_trips": len(matching_trips),
                })
            else:
                results.append({
                    "z_zonekey": zone_row["z_zonekey"],
                    "pickup_zone": zone_row["z_name"],
                    "avg_duration": np.nan,
                    "avg_distance": np.nan,
                    "num_trips": 0,
                })

        result_df = pd.DataFrame(results)
        return result_df.sort_values(
            ["avg_duration", "z_zonekey"], ascending=[False, True], na_position="last"
        ).reset_index(drop=True)
    finally:
        runner.disconnect()


def q11(data_paths: dict[str, str]) -> DataFrame:
    """Q11 (Milvus): Count trips that cross between different zones.

    Uses ST_Within for both pickup and dropoff zone matching.
    """
    runner = _get_runner(data_paths)
    try:
        # Get all zones
        zone_results = runner._query(
            "zone",
            "",
            ["z_zonekey", "z_boundary"],
            limit=1000000,
        )

        if not zone_results:
            return pd.DataFrame({"cross_zone_trip_count": [0]})

        zone_df = pd.DataFrame(zone_results)

        # Get all trips
        trip_results = runner._query(
            "trip",
            "",
            ["t_tripkey", "t_pickuploc", "t_dropoffloc"],
            limit=10000000,
        )

        if not trip_results:
            return pd.DataFrame({"cross_zone_trip_count": [0]})

        trip_df = pd.DataFrame(trip_results)

        # Parse geometries
        from shapely import wkt as shapely_wkt
        trip_df["pickup_geom"] = trip_df["t_pickuploc"].apply(shapely_wkt.loads)
        trip_df["dropoff_geom"] = trip_df["t_dropoffloc"].apply(shapely_wkt.loads)
        zone_df["zone_geom"] = zone_df["z_boundary"].apply(shapely_wkt.loads)

        # Build spatial index for zones
        zones = zone_df.to_dict("records")

        def find_zone(point):
            """Find zone containing the point."""
            for zone in zones:
                if zone["zone_geom"].contains(point):
                    return zone["z_zonekey"]
            return None

        # Find pickup and dropoff zones for each trip
        trip_df["pickup_zone"] = trip_df["pickup_geom"].apply(find_zone)
        trip_df["dropoff_zone"] = trip_df["dropoff_geom"].apply(find_zone)

        # Count cross-zone trips
        mask = (
            trip_df["pickup_zone"].notna() &
            trip_df["dropoff_zone"].notna() &
            (trip_df["pickup_zone"] != trip_df["dropoff_zone"])
        )
        count = int(mask.sum())

        return pd.DataFrame({"cross_zone_trip_count": [count]})
    finally:
        runner.disconnect()


def q12(data_paths: dict[str, str]) -> DataFrame:
    """Q12 (Milvus): NOT SUPPORTED - requires ST_KNN.

    K-Nearest Neighbor spatial join is not available in Milvus GIS.
    """
    raise NotImplementedError(
        "Q12 is not supported by Milvus GIS. "
        "Required function ST_KNN is not available."
    )
