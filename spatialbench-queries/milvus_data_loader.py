#!/usr/bin/env python3
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
Milvus Data Loader for SpatialBench.

This script loads SpatialBench benchmark data (Parquet files) into Milvus collections,
converting WKB geometry data to WKT format and creating appropriate indexes.

Usage:
    python milvus_data_loader.py --data-dir sf1-data --uri http://localhost:19530 --prefix spatialbench
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient
from shapely import wkb

# Default collection prefix
DEFAULT_PREFIX = "spatialbench"

# Table definitions with their geometry columns
TABLE_CONFIGS = {
    "trip": {
        "geometry_cols": ["t_pickuploc", "t_dropoffloc"],
        "primary_key": "t_tripkey",
        "schema_fields": [
            ("t_tripkey", DataType.INT64, True),  # primary key
            ("t_custkey", DataType.INT64, False),
            ("t_driverkey", DataType.INT64, False),
            ("t_vehiclekey", DataType.INT64, False),
            ("t_pickuptime", DataType.VARCHAR, False, 64),
            ("t_dropofftime", DataType.VARCHAR, False, 64),
            ("t_pickuploc", DataType.GEOMETRY, False),
            ("t_dropoffloc", DataType.GEOMETRY, False),
            ("t_distance", DataType.DOUBLE, False),
            ("t_fare", DataType.DOUBLE, False),
            ("t_tip", DataType.DOUBLE, False),
            ("t_totalamount", DataType.DOUBLE, False),
        ],
    },
    "customer": {
        "geometry_cols": [],
        "primary_key": "c_custkey",
        "schema_fields": [
            ("c_custkey", DataType.INT64, True),
            ("c_name", DataType.VARCHAR, False, 256),
            ("c_phone", DataType.VARCHAR, False, 64),
            ("c_email", DataType.VARCHAR, False, 256),
        ],
    },
    "driver": {
        "geometry_cols": [],
        "primary_key": "d_driverkey",
        "schema_fields": [
            ("d_driverkey", DataType.INT64, True),
            ("d_name", DataType.VARCHAR, False, 256),
            ("d_license", DataType.VARCHAR, False, 64),
            ("d_phone", DataType.VARCHAR, False, 64),
        ],
    },
    "vehicle": {
        "geometry_cols": [],
        "primary_key": "v_vehiclekey",
        "schema_fields": [
            ("v_vehiclekey", DataType.INT64, True),
            ("v_make", DataType.VARCHAR, False, 128),
            ("v_model", DataType.VARCHAR, False, 128),
            ("v_year", DataType.INT64, False),
            ("v_plate", DataType.VARCHAR, False, 32),
        ],
    },
    "building": {
        "geometry_cols": ["b_boundary"],
        "primary_key": "b_buildingkey",
        "schema_fields": [
            ("b_buildingkey", DataType.INT64, True),
            ("b_name", DataType.VARCHAR, False, 512),
            ("b_type", DataType.VARCHAR, False, 128),
            ("b_boundary", DataType.GEOMETRY, False),
        ],
    },
    "zone": {
        "geometry_cols": ["z_boundary"],
        "primary_key": "z_zonekey",
        "schema_fields": [
            ("z_zonekey", DataType.INT64, True),
            ("z_name", DataType.VARCHAR, False, 512),
            ("z_type", DataType.VARCHAR, False, 128),
            ("z_boundary", DataType.GEOMETRY, False),
        ],
    },
}


def wkb_to_wkt(wkb_data: bytes | None) -> str | None:
    """Convert WKB geometry to WKT format."""
    if wkb_data is None:
        return None
    try:
        geom = wkb.loads(wkb_data)
        return geom.wkt
    except Exception as e:
        print(f"Warning: Failed to convert WKB to WKT: {e}")
        return None


def get_parquet_path(data_dir: Path, table_name: str) -> Path | None:
    """Get the path to parquet file(s) for a table.

    Supports:
    1. Directory format: table_name/*.parquet
    2. Single file format: table_name.parquet
    """
    table_dir = data_dir / table_name
    if table_dir.is_dir():
        parquet_files = list(table_dir.glob("*.parquet"))
        if parquet_files:
            return table_dir
    single_file = data_dir / f"{table_name}.parquet"
    if single_file.exists():
        return single_file
    return None


def load_parquet_data(path: Path) -> pd.DataFrame:
    """Load data from parquet file(s)."""
    if path.is_dir():
        # Read all parquet files in directory
        parquet_files = sorted(path.glob("*.parquet"))
        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)
    return pd.read_parquet(path)


def create_collection_schema(table_name: str) -> CollectionSchema:
    """Create Milvus collection schema for a table."""
    config = TABLE_CONFIGS[table_name]
    fields = []

    for field_def in config["schema_fields"]:
        name = field_def[0]
        dtype = field_def[1]
        is_primary = field_def[2]

        if dtype == DataType.VARCHAR:
            max_length = field_def[3] if len(field_def) > 3 else 256
            field = FieldSchema(
                name=name,
                dtype=dtype,
                is_primary=is_primary,
                max_length=max_length,
            )
        elif dtype == DataType.GEOMETRY:
            # Geometry fields don't have max_length in the same way
            field = FieldSchema(
                name=name,
                dtype=dtype,
                is_primary=is_primary,
            )
        else:
            field = FieldSchema(
                name=name,
                dtype=dtype,
                is_primary=is_primary,
            )
        fields.append(field)

    return CollectionSchema(
        fields=fields,
        description=f"SpatialBench {table_name} table",
        enable_dynamic_field=True,
    )


def prepare_row_data(df: pd.DataFrame, table_name: str) -> list[dict[str, Any]]:
    """Prepare DataFrame rows for Milvus insertion."""
    config = TABLE_CONFIGS[table_name]
    geometry_cols = config["geometry_cols"]

    # Convert geometry columns from WKB to WKT
    for col in geometry_cols:
        if col in df.columns:
            df[col] = df[col].apply(wkb_to_wkt)

    # Convert timestamp columns to string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    # Handle NaN values
    df = df.fillna({
        col: "" for col in df.select_dtypes(include=["object"]).columns
    })
    df = df.fillna(0)

    return df.to_dict("records")


def load_table_to_milvus(
    client: MilvusClient,
    data_dir: Path,
    table_name: str,
    prefix: str,
    batch_size: int = 10000,
) -> int:
    """Load a single table into Milvus.

    Returns the number of rows loaded.
    """
    collection_name = f"{prefix}_{table_name}"

    # Get data path
    data_path = get_parquet_path(data_dir, table_name)
    if data_path is None:
        print(f"  Warning: No data found for table '{table_name}', skipping")
        return 0

    print(f"  Loading {table_name} from {data_path}...")

    # Load data
    df = load_parquet_data(data_path)
    print(f"    Loaded {len(df)} rows")

    # Drop existing collection if exists
    if client.has_collection(collection_name):
        print(f"    Dropping existing collection '{collection_name}'")
        client.drop_collection(collection_name)

    # Create collection with schema
    # Note: For simplicity, we use auto_id=False and let Milvus use the schema
    # In practice, you might want to customize this further
    schema = create_collection_schema(table_name)
    client.create_collection(
        collection_name=collection_name,
        schema=schema,
    )
    print(f"    Created collection '{collection_name}'")

    # Prepare and insert data in batches
    rows = prepare_row_data(df.copy(), table_name)
    total_inserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        client.insert(
            collection_name=collection_name,
            data=batch,
        )
        total_inserted += len(batch)
        print(f"    Inserted {total_inserted}/{len(rows)} rows", end="\r")

    print(f"    Inserted {total_inserted} rows total")

    # Create R-Tree index for geometry columns
    config = TABLE_CONFIGS[table_name]
    for geom_col in config["geometry_cols"]:
        print(f"    Creating R-Tree index on '{geom_col}'...")
        try:
            index_params = {
                "index_type": "RTREE",
                "metric_type": "",  # Not applicable for geometry
                "params": {},
            }
            client.create_index(
                collection_name=collection_name,
                field_name=geom_col,
                index_params=index_params,
            )
            print(f"    Created R-Tree index on '{geom_col}'")
        except Exception as e:
            print(f"    Warning: Failed to create R-Tree index on '{geom_col}': {e}")

    # Load collection into memory for queries
    client.load_collection(collection_name)
    print(f"    Collection '{collection_name}' loaded into memory")

    return total_inserted


def main():
    parser = argparse.ArgumentParser(
        description="Load SpatialBench data into Milvus collections"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Path to directory containing benchmark data (parquet files)",
    )
    parser.add_argument(
        "--uri",
        type=str,
        default="http://localhost:19530",
        help="Milvus server URI (default: http://localhost:19530)",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=DEFAULT_PREFIX,
        help=f"Collection name prefix (default: {DEFAULT_PREFIX})",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Comma-separated list of tables to load (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for insertion (default: 10000)",
    )

    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Data directory '{data_dir}' does not exist")
        sys.exit(1)

    # Determine tables to load
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",")]
        for t in tables:
            if t not in TABLE_CONFIGS:
                print(f"Error: Unknown table '{t}'")
                print(f"Available tables: {', '.join(TABLE_CONFIGS.keys())}")
                sys.exit(1)
    else:
        tables = list(TABLE_CONFIGS.keys())

    print(f"Connecting to Milvus at {args.uri}...")
    client = MilvusClient(uri=args.uri)

    print(f"\nLoading tables with prefix '{args.prefix}':")
    print(f"  Tables: {', '.join(tables)}")
    print(f"  Data directory: {data_dir}")
    print()

    total_rows = 0
    for table in tables:
        rows = load_table_to_milvus(
            client=client,
            data_dir=data_dir,
            table_name=table,
            prefix=args.prefix,
            batch_size=args.batch_size,
        )
        total_rows += rows
        print()

    print(f"Done! Loaded {total_rows} total rows across {len(tables)} tables")

    # List all collections
    print("\nCreated collections:")
    collections = client.list_collections()
    for coll in collections:
        if coll.startswith(args.prefix):
            stats = client.get_collection_stats(coll)
            print(f"  {coll}: {stats.get('row_count', 'N/A')} rows")

    client.close()


if __name__ == "__main__":
    main()
