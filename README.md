# AIS Vessel Collision Detector

Identifies two vessels with closest physical proximity (collision event) from Danish AIS data for **December 2021**, within a **50 nautical mile radius** of `55.225°N, 14.245°E`. Outputs their MMSI numbers, vessel names, collision timestamp, coordinates, and a trajectory map.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | ≥ 24 | https://docs.docker.com/get-docker/ |

No local Python or Java installation is needed — everything runs inside Docker.

---

## Project Structure

```
.
├── Dockerfile                  # Container definition (Spark + PySpark)
├── docker-compose.yml          # Convenience wrapper
├── requirements.txt            # Python deps (for reference)
├── README.md                   # This file
├── REPORT.md                   # Written methodology report
├── collision_detector.py       # Main PySpark pipeline
├── output/                     # ← Results appear here after run
│   ├── collision_map.html      # Interactive trajectory map
│   ├── results.txt             # Collision summary (MMSI, name, time, coords)
│   └── map.png                 # Screenshot of collision map
└── ais-collision-detector/
    └── data/                   # ← Put your AIS CSV files here
        └── aisdk-2021-12.csv
```

Note: the repository root contains `ais-collision-detector/data/`, and Docker Compose mounts `./ais-collision-detector/data` into `/data`.

---

## Data Setup

1. Go to **http://aisdata.ais.dk/**
2. Download the Danish AIS data for **December 2021** (file name will be something like `aisdk-2021-12.csv`)
3. Place the CSV file(s) inside the `ais-collision-detector/data/` folder

```
ais-collision-detector/
└── data/
    └── aisdk-2021-12.csv
```

The pipeline accepts multiple CSV files in the `ais-collision-detector/data/` folder (wildcarded as `*.csv`).

---

## Build & Run

### Docker Compose

```bash
# 1. Clone the repo
git clone https://github.com/EgleRim/Big_data_exam.git
cd ais-collision-detector

# 2. Place AIS CSV(s) in data/

# 3. Build the image
docker compose build

# 4. Run the detector
docker compose up

# Results appear in output/
```

---

## Expected Output

After a successful run you will find in `output/`:

| File | Description |
|------|-------------|
| `results.txt` | Plain-text summary: MMSI, vessel names, timestamp, coordinates, distance |
| `collision_map.html` | Interactive Folium map — open in any browser |

Console will also print:

```
============================================================
COLLISION EVENT FOUND
  MMSI A    : 123456789  (VESSEL NAME A)
  MMSI B    : 987654321  (VESSEL NAME B)
  Timestamp : 2021-12-XX HH:MM:SS
  Latitude  : XX.XXXXXX
  Longitude : XX.XXXXXX
  Distance  : 0.0XXX nm  (XX m)
============================================================
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_PATH` | `/data` | Path to directory with AIS CSV files |
| `OUTPUT_DIR` | `/output` | Path to write results and map |

Override at runtime: `docker run -e DATA_PATH=/custom/path ...`

---

## Performance Notes

- **Geohash bucketing** replaces a full O(n²) Cartesian self-join with a bucketed join, reducing complexity to roughly O(n · k) where k is the average bucket density.

---

## Link to Docker Hub

```bash
docker tag ais-collision-detector <your-username>/ais-collision-detector:latest
docker push <your-username>/ais-collision-detector:latest
```
