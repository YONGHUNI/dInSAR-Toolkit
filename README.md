# dInSAR-Toolkit
A Python-based automated dInSAR processing toolkit for surface deformation analysis using Sentinel-1 and ISCE2

**Still work in progress!!**

# 🛰️ Modular ISCE2 Sentinel-1 InSAR Pipeline

  

Automated, modular, and robust wrapper around the ISCE2 `topsApp.py` workflow for Sentinel-1 InSAR processing.

Designed for reproducible end-to-end processing: **search → download → preprocess → run ISCE → visualize displacement**.

  

This document combines the project overview and example notebook guide into a single, structured README.

  

---

  

# 🌟 Key Features

  

- **Modular Architecture**

- Separate managers for SLC search/download, orbit retrieval, DEM preparation, and ISCE execution.

- **Robust DEM Strategy**

- Automatically builds a DEM covering the full SLC intersection + configurable buffer.

- Prevents `runTopo` edge errors in mountainous or high-relief terrain.

- **ROI-Focused Processing**

- Processing window is limited to the user ROI for stability and speed.

- **Automated Auxiliary Data**

- Precise orbits (POEORB) with RESORB fallback.

- DEM download via `dem_stitcher` (GLO-30 / SRTM).

- **Modern Python Environment**

- Python 3.10 + Conda compatible.

- Notebook-friendly workflow.

  

---

  

# 🏗️ Processing Workflow

  

```mermaid

graph TD

User[User Configuration] -->|ROI & Dates| SLC[SLC Manager]

SLC -->|Scene Metadata| Orbit[Orbit Manager]

SLC -->|Geometry Bounds| DEM[DEM Manager]

SLC -->|Inputs| Proc[ISCE Processor]

Orbit -->|Orbit Files| Proc

DEM -->|Buffered DEM| Proc

Proc -->|Generate topsApp.xml| App[ISCE topsApp.py]

App --> Outputs[InSAR Products]

````

  

---

  

# 📂 Project Structure

  

```

./
├── main.py
├── environment.yml
├── README.md
├── modules/
│ ├── __init__.py
│ ├── SLC_manager.py
│ ├── orbit_manager.py
│ ├── DEM_manager.py
│ ├── isce_processor.py
│ └── auth_base.py
└── examples/
  ├── ISCE_processor(INSAR_main).ipynb
  ├── S1SLCManager.ipynb
  ├── OrbitManager.ipynb
  ├── DEMManager.ipynb
  └── README.md

```

  

---

  

# 🚀 Getting Started

  

## 1️⃣ Installation

  

```bash

git clone https://github.com/YONGHUNI/dInSAR-Toolkit.git

cd dInSAR-Toolkit

  

conda env create -f environment.yml

conda activate insar_env

```

  

---

  

## 2️⃣ Configuration (main.py)

  

```python

ROI_WKT = "POLYGON ((129.2 42.2, 129.3 42.2, 129.3 42.3, 129.2 42.3, 129.2 42.2))"

  

START_DATE = "2025-12-20"

END_DATE = "2026-01-10"

  

DEM_BUFFER = 0.2

AZIMUTH_LOOKS = 7

RANGE_LOOKS = 19

```

  

---

  

## 3️⃣ Run Pipeline

  

```bash
# this is a sample run
python main.py

```

  

---

  

## 4️⃣ Credentials

  

A **NASA Earthdata** account is required.

  

The pipeline will use:

  

* `.netrc` if present, or

* interactive login on first run.

  

---

  

# 📊 Outputs

  

Generated under:

  

```

insar_workspace/merged/

```

  

Key products:

  

| File | Meaning |
| ------------------------ | ---------------------------------------------- |
|  `filt_topophase.unw.geo`  | Geocoded unwrapped phase (displacement signal) |
|  `topophase.cor.geo`  | Coherence map |
|  `phsig.cor.geo`  | Phase sigma (uncertainty proxy) |

  

---

  

# 📚 Example Notebooks

  

The `examples/` directory contains interactive notebooks demonstrating each module and the full pipeline.

  

## Notebook Overview

  

| Notebook | Purpose |
| ---------------------------------- | ------------------------------------------------------------------ |
|  `S1SLCManager.ipynb`  | ASF search & Sentinel-1 download tests |
|  `OrbitManager.ipynb`  | Orbit retrieval tests |
|  `DEMManager.ipynb`  | DEM buffering and stitching demo |
|  `ISCE_processor(INSAR_main).ipynb`  | Interactive version of `main.py`  |
 
---

  

# ▶️ Running Example Notebooks

  

## Environment

  

```bash

conda activate insar_env

```

  

## Launch Jupyter

  

```bash

cd examples

jupyter lab

```

  

---

  

# 🧪 Pipeline Demo Notebook Flow

  ISCE_processor(INSAR_main).ipynb

## Step 1 — Configuration

  

* Define ROI (WKT)

* Set date range

* Set buffer and looks

  

## Step 2 — Data Acquisition

  

* SLC search and download

* Orbit files auto-retrieved

  

## Step 3 — DEM Preparation

  

**Large Buffer Strategy**

  

* DEM covers full SLC intersection

* Adds buffer (default 0.2°)

* Prevents edge artifacts

  

## Step 4 — ISCE Execution

  

* Generate `topsApp.xml`

* Run `topsApp.py`

  

Typical runtime: **60+ minutes**  on 12 core CPU machine.

  

---

  

# 📈 Visualization & Scientific Checks

  

## Coherence Check

  

File: `topophase.cor.geo`

  

| Coherence | Interpretation |
| --------- | --------------------------------- |
| > 0.5 | Reliable phase (urban, bare soil) |
| < 0.3 | Noisy (vegetation, water) |

  

---

  

## Displacement Conversion

  

Unwrapped phase → displacement:

  

```

displacement = phase * (λ / 4π)

```

  

$$
\Delta d_{\mathrm{LOS}} = \frac{\lambda}{4\pi} \Delta\phi
$$

  

To convert in meters

  

---

  

# 🛠️ Troubleshooting

  
  

## DEM Outside Coverage Error

  

Increase buffer:

  

```python

DEM_BUFFER = 0.3

```

  

---

  

## Authentication Failures

  

Check:

  

* Earthdata credentials

* `.netrc` permissions

* ASF / Earthdata login status

  

---

  

# 💾 Storage Requirements

  

Approximate disk usage:


| Component | Size |
| ------------------ | ---------------- |
| Sentinel-1 SLC | 4–8 GB per scene |
| ISCE intermediates | 50–100 GB |
| DEM tiles | 1–3 GB |

  

Ensure sufficient free space before processing.

  

---

  

# 📜 License

  

See `LICENSE` file for terms.

  

```

```
