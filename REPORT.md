# Methodology Report — AIS Vessel Collision Detection

## 1. Overview

This report describes the approach taken to identify a vessel collision event from Danish AIS data (December 2021) within a 50 nautical mile radius of 55.225°N, 14.245°E, using Apache Spark (PySpark) running inside Docker.

---

## 2. Data Source & Loading

**Source:** Danish Maritime Authority AIS data from [aisdata.ais.dk](http://aisdata.ais.dk/)

The dataset consists of CSV files with fields including MMSI, timestamp, latitude, longitude, speed over ground (SOG), course over ground (COG), navigational status, and vessel name. December 2021 data typically contains several million rows.

Spark's `spark.read.csv` with `header=true` and `mode=PERMISSIVE` was used. `PERMISSIVE` mode prevents a single malformed row from aborting the entire job — corrupt rows receive `null` values and are later dropped.

---

## 3. Data Cleaning & Preprocessing

### 3.1 Type Normalisation

Column names in the Danish AIS dataset vary slightly across monthly files (e.g. `# Timestamp` vs `Timestamp`). The pipeline uses a candidate-list mapping to resolve this robustly, then casts all columns to their correct Spark types.

Timestamps are parsed with two format patterns (`dd/MM/yyyy HH:mm:ss` and ISO-8601), using `coalesce` to try both — the first non-null result is kept.

### 3.2 Validity Range Filtering

| Field | Valid range | Rationale |
|-------|-------------|-----------|
| MMSI | 100 000 000 – 999 999 999 | Valid 9-digit maritime identifiers |
| Latitude | −90 to 90 | Physical bounds |
| Longitude | −180 to 180 | Physical bounds |
| SOG | 0 to 50 knots | Eliminates extreme GPS velocity artefacts, if present |

The pipeline also filters out known rescue and patrol vessels by checking vessel names for keywords such as `KBV`, `RESCUE`, `SAR`, `PILOT`, and `TUG`, preventing emergency and government craft from biasing the collision search.

---

## 4. Spatial & Temporal Filtering

A bounding box derived from the 50 nm radius is applied first (cheap, no UDF), then a precise Spark-native Haversine expression filters out corner pixels of the bounding box. This two-stage approach avoids calling the costly distance expression on the entire dataset.

The Haversine formula was chosen over simple Euclidean distance because longitude degrees shrink as latitude increases (~0.57× at 55°N), making Euclidean distance unreliable for anything beyond a few km.

Only records timestamped within **December 1–31, 2021** are retained.

---

## 5. Noise & Stationary Vessel Exclusion

### 5.1 GPS Jump Anomaly Filter

A Spark Window function partitions data by MMSI and orders by timestamp. For each ping, the distance to the *previous* ping is computed. Any record requiring a jump greater than **5 nm since the last ping** is discarded — such a displacement is physically impossible for a ship in a single AIS reporting cycle and indicates a GPS ghost position.

This is the key guard against falsely identifying a teleporting vessel as a collision.

### 5.2 Transit Speed Filter

Vessels at anchor or alongside a berth broadcast AIS but do not move. Two vessels at adjacent berths could be within 0.1 nm of each other continuously — these must not be flagged as a collision.

The pipeline uses a stricter operational filter:
- if SOG exists, only pings with **5.0 ≤ SOG ≤ 50.0 knots** are retained, removing idling and slow-moving vessels from the collision search,
- pings that imply a sequential GPS jump of more than **5.0 nm** from the prior ping for the same MMSI are dropped.

This ensures the algorithm focuses on actively underway vessels and avoids false collisions caused by stationary or drifting AIS targets.

---

## 6. Collision Detection Algorithm

### 6.1 Exact timestamp deduplication

Before any join, duplicate AIS records with identical `(mmsi, ts)` are removed using `row_number()` over a window. This preserves the original high-resolution timestamps and prevents spurious duplicate pairs, while leaving each unique second of telemetry intact.

### 6.2 Geohash Bucketing (Efficiency Rationale)

A naive self-join of N rows against N rows is O(n²) — unacceptable for millions of records. Instead:

1. Each ping is assigned to a spatial bucket by truncating lat/lon to 0.15° (~9 nm per bucket side).
2. The self-join matches each ping against pings in the same bucket and the eight neighbouring buckets, handling edge effects without physically duplicating rows.
3. The join also requires `a.ts == b.ts` and `a.mmsi < b.mmsi`, so only same-second, ordered vessel pairs are compared.

This reduces the effective join to O(n · k) where k is the average number of vessels in the local bucket at that second.

### 6.3 Distance Computation & Collision Threshold

After bucketing narrows candidates, the Spark-native Haversine expression computes exact great-circle distances for all remaining same-second pairs. The event with the smallest distance is selected from those exact-second matches.

If the closest exact-second pair exceeds 0.1 nm, the pipeline still returns the globally closest observed approach in the search window.

---

## 7. Trajectory Visualisation

The trajectories are extracted for each vessel in a **±10-minute window** around the collision timestamp. A Folium interactive HTML map is generated with:

- A dark CartoDB basemap (nautical feel, high contrast)
- Blue polyline for Vessel A, red for Vessel B
- Circle markers at track start/end
- A red exclamation icon at the closest approach point
- An information legend showing MMSI, vessel names, timestamp, coordinates, and distance

---

## 8. Computational Strategy Summary

| Optimisation | Impact |
|---|---|
| Bounding box pre-filter before Haversine UDF | Reduces UDF calls by ~95% |
| Exact-timestamp deduplication before join | Removes duplicate telemetry rows while preserving high-resolution data |
| Geohash bucketing with neighbouring-bucket join | Replaces O(n²) Cartesian with O(n·k) local join |
| `mmsi_a < mmsi_b` inequality in join | Eliminates duplicate (A,B)/(B,A) pairs |
| DataFrame caching after preprocessing | Avoids recomputing the full pipeline for the trajectory extraction step |
| `spark.sql.adaptive.enabled=true` | Auto-coalesces shuffle partitions based on actual data distribution |

---

## 9. Findings

*(Fill this section in after running the pipeline with your data.)*

```
Vessel A MMSI : ___________
Vessel A Name : ___________
Vessel B MMSI : ___________
Vessel B Name : ___________
Timestamp     : 2021-12-XX HH:MM:SS UTC
Latitude      : XX.XXXXXX
Longitude     : XX.XXXXXX
Distance      : X.XXXX nm (XXX m)
```

The trajectory map (`output/collision_map.html`) shows the approach paths of both vessels converging at the collision point and diverging (or stopping) afterwards.
