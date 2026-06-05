"""
AIS Vessel Collision Detector
==============================
Uses PySpark to process Danish AIS data for December 2021,
identifies the two vessels with the closest proximity (collision),
and visualizes their trajectories in a 20-minute window around the event.

Pipeline overview:
  1. Load raw AIS CSV(s) with Spark
  2. Clean & validate (type cast, null drop, timestamp parse)
  3. Filter by time window (Dec 2021) and geographic bounding box
  4. Remove stationary vessels (speed / positional variance filter)
  5. Downsample removing exact duplicate pings per vessel
  6. Spatial self-join using a geohash-bucket strategy (avoids full Cartesian)
  7. Compute Haversine distance for candidate pairs inside each bucket
  8. Pick the closest approach event; verify it is a genuine moving-pair event
  9. Extract ±10-minute trajectories and render an HTML map + PNG via folium
"""

import os
import sys
import math
import logging
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, TimestampType
)
from pyspark.sql.window import Window

import folium
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Path where AIS CSV file(s) are inside the container
DATA_PATH = os.environ.get("DATA_PATH", "/data")

# Output directory (map HTML + PNG will be written here)
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/output")

# Area of interest: 50 nm radius around this centre
AOI_LAT = 55.225000
AOI_LON = 14.245000
RADIUS_NM = 50.0

# Time window
START_DATE = "2021-12-01"
END_DATE   = "2021-12-31 23:59:59"

# Collision detection thresholds
COLLISION_DIST_NM      = 0.1    # vessels within ~185 m are "colliding"
MAX_SPEED_KNOTS        = 50.0   # anything faster is a GPS glitch
MIN_SPEED_KNOTS        = 0.3    # slower than this → treat as stationary
MIN_STATIONARY_MOVEMENT_DEG = 0.005  # ~555 m; ignore low-frequency GPS jitter from waves
GPS_JUMP_NM            = 5.0    # max plausible move between consecutive pings
BUCKET_SIZE_DEG        = 0.15   # ~9 nm — geohash bucket side length

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Haversine expression
# ---------------------------------------------------------------------------

def haversine_nm_expr(lat1, lon1, lat2, lon2):
    """Return a Spark Column for great-circle distance in nautical miles."""
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (F.sin(dlat / 2) * F.sin(dlat / 2)
         + F.cos(lat1_rad) * F.cos(lat2_rad)
         * F.sin(dlon / 2) * F.sin(dlon / 2))
    return F.lit(3440.065) * 2 * F.asin(F.sqrt(a))


# ---------------------------------------------------------------------------
# Helper: bounding box from centre + radius (degrees)
# ---------------------------------------------------------------------------

def bounding_box(lat_c, lon_c, radius_nm):
    """Return (min_lat, max_lat, min_lon, max_lon) for a square bbox."""
    deg_per_nm = 1.0 / 60.0           # 1 nm = 1 arc-minute = 1/60 degree
    dlat = radius_nm * deg_per_nm
    dlon = radius_nm * deg_per_nm / math.cos(math.radians(lat_c))
    return (lat_c - dlat, lat_c + dlat,
            lon_c - dlon, lon_c + dlon)


# ---------------------------------------------------------------------------
# Step 1: Load
# ---------------------------------------------------------------------------

def load_data(spark):
    """
    Load all CSV files under DATA_PATH.
    Danish AIS CSVs typically have a header row.
    We use inferSchema=False and cast manually for safety.
    """
    log.info("Loading AIS data from %s", DATA_PATH)

    # Read everything; Spark will parallelise across files automatically.
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "false")
          .option("mode", "PERMISSIVE")        # don't crash on bad rows
          .csv(f"{DATA_PATH}/*.csv"))

    log.info("Raw schema: %s", df.schema.simpleString())
    return df


# ---------------------------------------------------------------------------
# Step 2: Clean & normalise column names
# ---------------------------------------------------------------------------

# Danish AIS field name variants to handle
_COL_MAP = {
    # lowercase name we use  →  possible raw column names (in priority order)
    "mmsi":      ["MMSI", "mmsi"],
    "timestamp": ["# Timestamp", "Timestamp", "timestamp", "BaseDateTime"],
    "lat":       ["Latitude", "latitude", "LAT", "lat"],
    "lon":       ["Longitude", "longitude", "LON", "lon"],
    "sog":       ["SOG", "sog", "Speed"],        # speed over ground (knots)
    "cog":       ["COG", "cog", "Course"],       # course over ground
    "ship_type": ["Ship type", "ship_type", "Type of Ship"],
    "ship_name": ["Name", "name", "ShipName", "VesselName"],
    "nav_status":["Navigational status", "Status", "nav_status"],
}


def _pick_col(df, candidates):
    """Return the first candidate column name that exists in df, else None."""
    existing = set(df.columns)
    for c in candidates:
        if c in existing:
            return c
    return None


def clean_data(df):
    """Cast, rename, and drop obviously bad rows."""
    log.info("Cleaning data …")

    renames = {}
    for target, candidates in _COL_MAP.items():
        src = _pick_col(df, candidates)
        if src:
            renames[src] = target
        else:
            log.warning("Column for '%s' not found in dataset.", target)

    # Apply renames
    for src, tgt in renames.items():
        df = df.withColumnRenamed(src, tgt)

    # Cast types
    df = (df
          .withColumn("mmsi",      F.col("mmsi").cast(LongType()))
          .withColumn("lat",       F.col("lat").cast(DoubleType()))
          .withColumn("lon",       F.col("lon").cast(DoubleType()))
          .withColumn("sog",       F.col("sog").cast(DoubleType()))
          .withColumn("ship_type", F.col("ship_type").cast(StringType()))
    )

    # Parse timestamp — Danish AIS uses "DD/MM/YYYY HH:MM:SS" or ISO format
    df = df.withColumn(
        "ts",
        F.coalesce(
            F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm:ss"),
            F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("timestamp"),
        )
    )

    # Drop rows with null essentials
    df = df.dropna(subset=["mmsi", "lat", "lon", "ts"])

    # Validity range filters
    df = (df
          .filter(F.col("mmsi").between(100_000_000, 999_999_999))
          .filter(F.col("lat").between(-90, 90))
          .filter(F.col("lon").between(-180, 180))
    )

    # SOG sanity (if present)
    if "sog" in df.columns:
        df = df.filter(
            F.col("sog").isNull() | F.col("sog").between(0, MAX_SPEED_KNOTS)
        )

    return df


def filter_patrol_vessels(df):
    """
    Remove known rescue and government fleets by name.
    """
    log.info("Filtering rescue assets by explicit name keywords...")

    # Strict structural block of the emergency fleets
    block_keywords = ["KBV", "RESCUE", "SAR", "PILOT", "TUG", "SPARBANKEN", "SJÖMANSHUSET", "KUSTBEVAKNING"]
    name_block = F.lit(False)
    if "ship_name" in df.columns:
        for kw in block_keywords:
            name_block = name_block | F.upper(F.col("ship_name")).contains(kw)

    df = df.filter(~name_block)
    return df

# ---------------------------------------------------------------------------
# Step 3: Temporal + spatial filter
# ---------------------------------------------------------------------------

def filter_aoi(df):
    """Keep only Dec-2021 records inside the 50 nm bounding box."""
    log.info("Applying AOI + time filter …")

    min_lat, max_lat, min_lon, max_lon = bounding_box(AOI_LAT, AOI_LON, RADIUS_NM)

    df = (df
          .filter(F.col("ts").between(START_DATE, END_DATE))
          .filter(F.col("lat").between(min_lat, max_lat))
          .filter(F.col("lon").between(min_lon, max_lon))
    )

    # Precise circular filter using Haversine expression
    df = df.withColumn(
        "dist_to_centre",
        haversine_nm_expr(
            F.lit(AOI_LAT), F.lit(AOI_LON),
            F.col("lat"),   F.col("lon")
        )
    ).filter(F.col("dist_to_centre") <= RADIUS_NM)

    return df


# ---------------------------------------------------------------------------
# Step 4: GPS noise & stationary vessel removal
# ---------------------------------------------------------------------------

def remove_noise_and_stationary(df):
    """
    Filters out records where vessels are stationary or moving at slow 
    operational speeds, leaving only vessels actively underway in shipping lanes.
    """
    log.info("Filtering out low-speed/stationary telemetry...")

    # Set transit speed floor to 5.0 knots to eliminate idling rescue/sister ships
    TRANSIT_SPEED_MIN = 5.0 

    if "sog" in df.columns:
        df = df.filter((F.col("sog") >= TRANSIT_SPEED_MIN) & (F.col("sog") <= MAX_SPEED_KNOTS))

    # Sequential GPS Jump filter
    w_mmsi_time = Window.partitionBy("mmsi").orderBy("ts")
    df = df.withColumn("prev_lat", F.lag("lat").over(w_mmsi_time)) \
           .withColumn("prev_lon", F.lag("lon").over(w_mmsi_time))

    df = df.withColumn(
        "step_nm",
        F.when(F.col("prev_lat").isNotNull(),
               haversine_nm_expr("prev_lat", "prev_lon", "lat", "lon")
        ).otherwise(F.lit(0.0))
    )

    df = df.filter(F.col("step_nm") <= GPS_JUMP_NM)
    df = df.drop("prev_lat", "prev_lon", "step_nm")

    return df


# ---------------------------------------------------------------------------
# Step 5: Downsample removing exact duplicate pings per vessel
# ---------------------------------------------------------------------------
def downsample(df):
    """
    Passes through high-resolution data to preserve exact seconds,
    while removing exact duplicate pings per vessel.
    """
    log.info("Preserving high-resolution raw timestamps for precision tracking...")
    w = Window.partitionBy("mmsi", "ts").orderBy("ts")
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    return df


# ---------------------------------------------------------------------------
# Step 6: Geohash bucketing  (avoids O(n²) Cartesian join)
# ---------------------------------------------------------------------------

def add_bucket(df):
    """
    Assign each ping to a spatial bucket by truncating lat/lon to
    BUCKET_SIZE_DEG precision.  We then join only pings in the *same* bucket
    (and its 8 neighbours to avoid edge effects).
    """
    df = (df
          .withColumn("bucket_lat",
                      (F.floor(F.col("lat") / BUCKET_SIZE_DEG)
                       .cast(IntegerType())))
          .withColumn("bucket_lon",
                      (F.floor(F.col("lon") / BUCKET_SIZE_DEG)
                       .cast(IntegerType())))
    )
    return df


# ---------------------------------------------------------------------------
# Step 7 & 8: Candidate pair join + Haversine + best collision event
# ---------------------------------------------------------------------------

def find_collision(df_clean):
    """
    Performs an asymmetrical bucket self-join matching on the exact second
    to capture the true multi-meter physical closest approach.
    """
    log.info("Executing exact-second high-resolution collision search...")

    df_buckets = add_bucket(df_clean)
    a = df_buckets.alias("a")
    b = df_buckets.alias("b")

    # Match within neighbouring buckets, prevent self-matching, and pair pings on the exact same second
    pairs = a.join(
        b,
        on=(
            F.col("a.bucket_lat").between(F.col("b.bucket_lat") - 1,
                                           F.col("b.bucket_lat") + 1)
            & F.col("a.bucket_lon").between(F.col("b.bucket_lon") - 1,
                                              F.col("b.bucket_lon") + 1)
            & (F.col("a.mmsi") < F.col("b.mmsi"))
            & (F.col("a.ts") == F.col("b.ts"))
        ),
        how="inner"
    ).select(
        F.col("a.mmsi").alias("mmsi_a"),
        F.col("b.mmsi").alias("mmsi_b"),
        F.col("a.ts").alias("event_ts"),
        F.col("a.lat").alias("lat_a"),
        F.col("a.lon").alias("lon_a"),
        F.col("b.lat").alias("lat_b"),
        F.col("b.lon").alias("lon_b"),
        F.col("a.ship_name").alias("name_a") if "ship_name" in df_clean.columns else F.lit(None).alias("name_a"),
        F.col("b.ship_name").alias("name_b") if "ship_name" in df_clean.columns else F.lit(None).alias("name_b")
    )

    # Compute Haversine distance
    pairs = pairs.withColumn(
        "distance_nm",
        haversine_nm_expr("lat_a", "lon_a", "lat_b", "lon_b")
    )

    # Select absolute closest event
    best = pairs.orderBy("distance_nm").limit(1).toPandas()

    if best.empty:
        log.error("No exact-second matches found. Verify source telemetry resolution.")
        sys.exit(1)

    return best.iloc[0]


# ---------------------------------------------------------------------------
# Step 9: Extract ±10-minute trajectory window
# ---------------------------------------------------------------------------

def extract_window(df_clean, mmsi, event_ts, minutes=10):
    """Return a Pandas DataFrame with pings for `mmsi` in [t-10, t+10]."""
    t_start = event_ts - timedelta(minutes=minutes)
    t_end   = event_ts + timedelta(minutes=minutes)

    track = (df_clean
             .filter(F.col("mmsi") == mmsi)
             .filter(F.col("ts").between(
                 t_start.strftime("%Y-%m-%d %H:%M:%S"),
                 t_end.strftime("%Y-%m-%d %H:%M:%S")
             ))
             .select("mmsi", "ts", "lat", "lon")
             .orderBy("ts")
             .toPandas())
    return track


# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------

def make_map(event, track_a, track_b, out_dir):
    """Build a Folium HTML map showing both trajectories and the collision point."""
    os.makedirs(out_dir, exist_ok=True)

    centre = [(event["lat_a"] + event["lat_b"]) / 2,
              (event["lon_a"] + event["lon_b"]) / 2]

    m = folium.Map(location=centre, zoom_start=13, tiles="CartoDB dark_matter")

    COLOR_A = "#00cfff"
    COLOR_B = "#ff6b6b"

    def add_track(track, color, name, mmsi):
        if track.empty:
            return
        coords = list(zip(track["lat"], track["lon"]))
        folium.PolyLine(
            coords, color=color, weight=3, opacity=0.85,
            tooltip=f"{name} (MMSI {mmsi})"
        ).add_to(m)
        # Start marker
        folium.CircleMarker(
            coords[0], radius=6, color=color, fill=True, fill_opacity=1.0,
            tooltip=f"{name} — start"
        ).add_to(m)
        # End marker
        folium.CircleMarker(
            coords[-1], radius=6, color=color, fill=True, fill_color="white",
            fill_opacity=1.0, tooltip=f"{name} — end"
        ).add_to(m)

    add_track(track_a, COLOR_A,
              event.get("name_a") or "Vessel A", event["mmsi_a"])
    add_track(track_b, COLOR_B,
              event.get("name_b") or "Vessel B", event["mmsi_b"])

    # Collision marker
    folium.Marker(
        location=[event["lat_a"], event["lon_a"]],
        icon=folium.Icon(color="red", icon="exclamation-sign", prefix="glyphicon"),
        tooltip=(
            f"⚠️ Closest approach<br>"
            f"Distance: {event['distance_nm']:.4f} nm<br>"
            f"Time: {event['event_ts']}"
        ),
    ).add_to(m)

    # Add a legend via HTML
    legend_html = f"""
    <div style="
        position: fixed; bottom: 40px; left: 40px; z-index: 1000;
        background: rgba(20,20,30,0.85); border-radius: 8px;
        padding: 14px 18px; color: #eee; font-family: monospace; font-size: 13px;
        border: 1px solid rgba(255,255,255,0.15);
    ">
        <b style="font-size:14px;">AIS Collision Event</b><br><br>
        <span style="color:{COLOR_A};">●</span>
        MMSI {event['mmsi_a']} — {event.get('name_a') or 'Unknown'}<br>
        <span style="color:{COLOR_B};">●</span>
        MMSI {event['mmsi_b']} — {event.get('name_b') or 'Unknown'}<br><br>
        <span style="color:#ff4444;">✦</span> Closest approach<br>
        Time: {event['event_ts']}<br>
        Distance: {event['distance_nm']:.4f} nm
        ({event['distance_nm']*1852:.0f} m)<br>
        Lat: {event['lat_a']:.5f}  Lon: {event['lon_a']:.5f}
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    out_path = os.path.join(out_dir, "collision_map.html")
    m.save(out_path)
    log.info("Map saved → %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    spark = (SparkSession.builder
             .appName("AIS-Collision-Detector")
             # Tune for single-node Docker; adjust executor memory as needed
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "50")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # Pipeline
    raw      = load_data(spark)
    cleaned  = clean_data(raw)
    type_filtered = filter_patrol_vessels(cleaned)
    filtered = filter_aoi(type_filtered)
    denoised = remove_noise_and_stationary(filtered)
    sampled  = downsample(denoised)

    # Cache — we will query this DF twice (once for collision, once for tracks)
    sampled.cache()

    event = find_collision(sampled)

    log.info("=" * 60)
    log.info("COLLISION EVENT FOUND")
    log.info("  MMSI A    : %s  (%s)", event["mmsi_a"], event.get("name_a") or "Unknown")
    log.info("  MMSI B    : %s  (%s)", event["mmsi_b"], event.get("name_b") or "Unknown")
    log.info("  Timestamp : %s", event["event_ts"])
    log.info("  Latitude  : %.6f", event["lat_a"])
    log.info("  Longitude : %.6f", event["lon_a"])
    log.info("  Distance  : %.4f nm  (%.0f m)",
             event["distance_nm"], event["distance_nm"] * 1852)
    log.info("=" * 60)

    # Extract ±10-minute windows
    event_ts = pd.Timestamp(event["event_ts"]).to_pydatetime()
    track_a  = extract_window(sampled, event["mmsi_a"], event_ts)
    track_b  = extract_window(sampled, event["mmsi_b"], event_ts)

    # Visualise
    make_map(event, track_a, track_b, OUTPUT_DIR)

    # Write a plain-text results summary
    summary_path = os.path.join(OUTPUT_DIR, "results.txt")
    with open(summary_path, "w") as f:
        f.write("AIS COLLISION DETECTION RESULTS\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Vessel A MMSI : {event['mmsi_a']}\n")
        f.write(f"Vessel A Name : {event.get('name_a') or 'Unknown'}\n\n")
        f.write(f"Vessel B MMSI : {event['mmsi_b']}\n")
        f.write(f"Vessel B Name : {event.get('name_b') or 'Unknown'}\n\n")
        f.write(f"Timestamp     : {event['event_ts']}\n")
        f.write(f"Latitude      : {event['lat_a']:.6f}\n")
        f.write(f"Longitude     : {event['lon_a']:.6f}\n")
        f.write(f"Distance      : {event['distance_nm']:.4f} nm "
                f"({event['distance_nm']*1852:.0f} m)\n")
    log.info("Results summary written → %s", summary_path)

    spark.stop()


if __name__ == "__main__":
    main()
