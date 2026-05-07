<p align="center">
  <img src="fave_cover_final.png" alt="FAVE Banner" width="100%"/>
</p>

<p align="center">
  <a href="#-about"><img src="https://img.shields.io/badge/IEEE_VIS-2026-E24B4A?style=for-the-badge&labelColor=091422" alt="IEEE VIS 2026"/></a>
  <a href="#-getting-started"><img src="https://img.shields.io/badge/Python-3.10+-1D9E75?style=for-the-badge&logo=python&logoColor=white&labelColor=091422" alt="Python"/></a>
  <a href="#-getting-started"><img src="https://img.shields.io/badge/Ollama-LLM-EF9F27?style=for-the-badge&labelColor=091422" alt="Ollama"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-85B7EB?style=for-the-badge&labelColor=091422" alt="License"/></a>
</p>

<p align="center">
  <b>A Visual Analytics System for Urban Accessibility Fairness</b><br/>
  <sub>Visual analytics for urban fairness across scales, travel modes, and planning scenarios</sub>
</p>

---

## 🗺️ About

**FAVE** is an interactive visual analytics system built on OpenStreetMap data that enables researchers and urban planners to explore, compare, and assess **accessibility fairness** across cities. It supports multiple travel modes, spatial scales, and what-if planning scenarios — powered by a local LLM through Ollama.

<p align="center">
  <a href="#-about"><img src="https://img.shields.io/badge/Växjö-5DCAA5?style=flat-square&labelColor=091422" alt="Växjö"/></a>
  <a href="#-about"><img src="https://img.shields.io/badge/Malmö-EF9F27?style=flat-square&labelColor=091422" alt="Malmö"/></a>
  <a href="#-about"><img src="https://img.shields.io/badge/Stockholm-85B7EB?style=flat-square&labelColor=091422" alt="Stockholm"/></a>
</p>

---

## 🎥 Demo

<p align="center">
  <a href="https://youtu.be/WvCThY4LkWk">
    <img src="https://img.youtube.com/vi/WvCThY4LkWk/maxresdefault.jpg" alt="FAVE Demo Video" width="80%"/>
  </a>
  <br/>
  <sub>▶️ Click to watch the demonstration video</sub>
</p>

---

## 🚀 Getting Started

### Prerequisites

| Tool | Purpose |
|------|---------|
| **Python 3.10+** | Backend API and data fetcher |
| **[Ollama](https://ollama.com)** | Local LLM inference |

### 1️⃣ Clone the repository

```bash
git clone https://github.com/claudiodgl/FAVE.git
cd FAVE
```

The repo only ships small static files (~12 MB). The large building geojsons (40 MB – 130 MB per city) are pulled on demand by the data fetcher in step 3.

### 2️⃣ Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate    # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3️⃣ Fetch the data files for the cities you want

```bash
# Just one city (recommended for first run)
python tools/fetch_city_data.py --city vaxjo

# Multiple cities
python tools/fetch_city_data.py --city malmo --city stockholm

# Everything (≈ 530 MB total)
python tools/fetch_city_data.py --all

# Inspect what's available without downloading
python tools/fetch_city_data.py --list
```

Files land in `frontend/assets/data/`. The fetcher is **idempotent** — re-run any time and it'll skip files that are already in place. To self-host the data, set `FAVE_DATA_BASE_URL` to your own URL prefix.

### 4️⃣ Pull the LLM model (one-time)

```bash
ollama serve          # start the Ollama daemon
ollama pull llama3.2  # download the model
```

### 5️⃣ Launch

Open **three terminals** and run:

| Terminal | Command | Description |
|----------|---------|-------------|
| **A** | `uvicorn api.server:app --host 127.0.0.1 --port 8001 --reload` | Backend API |
| **B** | `cd frontend && python3 -m http.server 5500` | Frontend server |
| **C** | `ollama serve` | LLM service _(if not already running)_ |

Then open the app:

🟢 **App** → [http://127.0.0.1:5500/index.html](http://127.0.0.1:5500/index.html)  
🔵 **Health check** → [http://127.0.0.1:8001/health](http://127.0.0.1:8001/health)

---

## 🏗️ Project Structure

```
FAVE/
├── api/                          # FastAPI backend
│   └── server.py
├── frontend/                     # Static frontend (HTML/JS)
│   ├── index.html
│   └── assets/
│       ├── js/                   # MVC modules (lib/models/views/controllers)
│       ├── css/
│       └── data/                 # Small static data (POIs, district boundaries,
│                                 # population JSON). Large building geojsons land
│                                 # here too after running fetch_city_data.py.
├── tools/
│   ├── fetch_city_data.py        # Per-city downloader (run on first setup)
│   ├── data_manifest.json        # Lists files + remote URLs per city
│   └── bake_city_data.py         # Offline POI baker (Overpass/Nominatim)
├── requirements.txt              # Python dependencies
└── README.md
```

> **Why the data fetcher?** Building geojsons range from 40 MB (Malmö) to 130 MB (Göteborg). Bundling them in git would push the repo well past GitHub's recommended limits, so they live in a Release on this repo and are pulled per-city on demand. Override `FAVE_DATA_BASE_URL` to host them yourself.

---

## 👥 Authors

<table>
  <tr>
    <td align="center"><b>Parisa Salmanian</b></td>
    <td align="center"><b>Benjamin Powley</b></td>
    <td align="center"><b>Rafael M. Martins</b></td>
    <td align="center"><b>Fernando Paulovich</b></td>
  </tr>
  <tr>
    <td align="center"><b>Andreas Kerren</b></td>
    <td align="center"><b>Amilcar Soares</b></td>
    <td align="center"><b>Nivan Ferreira</b></td>
    <td align="center"><b>Claudio D. G. Linhares</b></td>
  </tr>
</table>

---

## 📄 License

This project is licensed under the **Apache License 2.0** — see the [LICENSE](LICENSE) file for details.

Code is released under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0). Other materials (data, figures, documents) are released under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), as recommended by IEEE VIS 2026.

---

<p align="center">
  <sub>Submitted to <b>IEEE VIS 2026</b> · Supplementary Video</sub><br/>
  <sub>Made with 🌿 for fairer cities</sub>
</p>
