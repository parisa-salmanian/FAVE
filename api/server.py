# C:\Users\pasaaa\city-newbuilds\api\server.py
import json, uuid, math, re, time, os
from pathlib import Path
from typing import Dict, Any, List, Tuple

import requests
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .ebm_service import router as ebm_router
from .fairness_routes import router as fairness_router
from .llm_routes import router as llm_router


ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "api_data"
JOBS = STORE / "jobs"
STATIC_ROOT = STORE / "static"
FRONTEND_ROOT = ROOT / "frontend"
FRONTEND_INDEX = FRONTEND_ROOT / "index.html"
for p in [STORE, JOBS, STATIC_ROOT]:
    p.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="New Builds Explorer API", version="0.3")


app.include_router(ebm_router)
app.include_router(llm_router)
app.include_router(fairness_router, prefix="/api")

# CORS: explicitly allow the frontend origin
# origins = [
#     "http://127.0.0.1:5500",
#     "http://localhost:5500",   # add this too in case you switch
# ]


# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,     # no "*"
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

cors_allow_all = os.getenv("CORS_ALLOW_ALL", "true").lower() in {"1", "true", "yes"}

cors_kwargs = {
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}
if cors_allow_all:
    cors_kwargs.update({"allow_origins": ["*"], "allow_credentials": False})
else:
    cors_kwargs.update(
        {
            "allow_origin_regex": r"^http://(localhost|127\.0\.0\.1)(:\d+)?$",
            "allow_credentials": True,
        }
    )

app.add_middleware(CORSMiddleware, **cors_kwargs)
app.mount("/static", StaticFiles(directory=str(STATIC_ROOT)), name="static")
if FRONTEND_ROOT.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_ROOT / "assets")), name="frontend_assets")

# ---------- Overpass mirrors (tried in order) ----------
OVERPASS_ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    # "https://overpass.nchc.org.tw/api/interpreter",
    "https://overpass.osm.ch/api/interpreter",
]
USER_AGENT = {"User-Agent": "newbuilds/1.0 (contact: parisa.salmanian.71@gmail.com)"}
NOMINATIM_ENDPOINTS = [
    "https://nominatim.openstreetmap.org/search",
    "https://nominatim.openstreetmap.fr/search",
    "https://nominatim.terralab.io/search",
]
NOMINATIM_TIMEOUT = (20, 40)
NOMINATIM_RETRY_DELAY = 1.0

def geocode_city(city: str) -> Dict[str, Any]:
    params = {"q": city, "format": "json", "limit": 1, "addressdetails": 1, "polygon_geojson": 0}
    last_error = None
    arr = None
    for url in NOMINATIM_ENDPOINTS:
        try:
            r = requests.get(url, params=params, headers=USER_AGENT, timeout=NOMINATIM_TIMEOUT)
            r.raise_for_status()
            arr = r.json()
            if arr:
                break
        except Exception as exc:
            last_error = exc
            time.sleep(NOMINATIM_RETRY_DELAY)
            continue
    if arr is None:
        raise HTTPException(status_code=504, detail=f"Nominatim timeout for: {city}") from last_error
    if not arr:
        raise HTTPException(status_code=404, detail=f"City not found: {city}")
    it = arr[0]
    lat = float(it["lat"]); lon = float(it["lon"])
    bb = [float(x) for x in it["boundingbox"]]  # [south, north, west, east]
    bbox = (bb[0], bb[2], bb[1], bb[3])        # (south, west, north, east)
    return {"lat": lat, "lon": lon, "bbox": bbox, "raw": it}

def _parse_height(tags: Dict[str, Any]) -> float:
    def parse_m(val: str) -> float:
        v = str(val or "").strip().lower().replace(",", ".")
        for tok in ["metres","meters","meter","metre","m"]:
            v = v.replace(tok, "")
        v = v.strip()
        try:
            return float(v)
        except Exception:
            return math.nan
    for key in ("height", "building:height"):
        if key in tags:
            h = parse_m(tags[key])
            if math.isfinite(h) and h > 0:
                return h
    if "building:levels" in tags:
        try:
            levels = float(str(tags["building:levels"]).replace(",", "."))
            if levels > 0: return round(levels * 3.0, 2)
        except Exception:
            pass
    return 10.0

def classify_building(tags: Dict[str, Any]) -> str:
    def g(key, default=""): return str(tags.get(key, default) or "").strip().lower()
    if not tags: return "unknown"
    bld = g("building"); amen = g("amenity"); shop = g("shop"); office = g("office")
    tourism = g("tourism"); leisure = g("leisure"); healthcare = g("healthcare")
    railway = g("railway"); aeroway = g("aeroway"); public_transport = g("public_transport")
    name = g("name"); has_address = any(k in tags for k in ("addr:housenumber","addr:street"))

    if shop == "supermarket" or amen == "supermarket" or "supermarket" in name: return "supermarket"
    building_map = {
        "house":"residential","detached":"residential","semidetached_house":"residential",
        "apartments":"residential","terrace":"residential","residential":"residential",
        "retail":"retail","commercial":"commercial","warehouse":"industrial",
        "industrial":"industrial","factory":"industrial","manufacture":"industrial",
        "school":"education","college":"education","university":"education","kindergarten":"education",
        "hospital":"health","clinic":"health","doctors":"health",
        "public":"public","civic":"public","townhall":"public","fire_station":"public","police":"public","library":"public",
        "church":"religious","mosque":"religious","temple":"religious","synagogue":"religious","place_of_worship":"religious",
        "train_station":"transportation","transportation":"transportation","transport":"transportation","parking":"transportation",
        "garage":"garage","sports_hall":"sports","stadium":"sports","hotel":"hotel",
        "farm":"farm","farm_auxiliary":"farm","barn":"farm",
    }
    if bld in building_map: return building_map[bld]
    amen_map = {
        "school":"education","college":"education","university":"education","kindergarten":"education",
        "hospital":"health","clinic":"health","doctors":"health","dentist":"health","pharmacy":"health",
        "townhall":"public","embassy":"public","police":"public","fire_station":"public","library":"public","community_centre":"public",
        "bus_station":"transportation","ferry_terminal":"transportation","bicycle_parking":"transportation","parking":"transportation",
        "place_of_worship":"religious",
    }
    if amen in amen_map: return amen_map[amen]
    if shop: return "retail"
    office_map = {"company":"commercial","it":"commercial","lawyer":"commercial","tax_advisor":"commercial","estate_agent":"commercial","accountant":"commercial","ngo":"public","government":"public"}
    if office in office_map: return office_map[office]
    if office: return "commercial"
    if tourism in ("hotel","guest_house","hostel","motel"): return "hotel"
    if leisure in ("sports_centre","stadium","sport_pitch","fitness_centre"): return "sports"
    if public_transport or railway or aeroway: return "transportation"
    if healthcare: return "health"
    if bld in ("yes","","roof"):
        if has_address and not (amen or shop or office or tourism or leisure):
            return "residential"
        return "unknown"
    return "unknown"

def build_overpass_query(bbox: Tuple[float,float,float,float]) -> str:
    south, west, north, east = bbox
    return f"""
    [out:json][timeout:120];
    (
      way["building"]({south},{west},{north},{east});
      relation["building"]({south},{west},{north},{east});
    );
    (._;>;);
    out body;
    """

def overpass_buildings(bbox: Tuple[float,float,float,float]) -> Dict[str, Any]:
    """
    Query Overpass with mirror-rotation and retries to avoid 504s.
    """
    query = build_overpass_query(bbox)
    return _overpass_query(query)

def _overpass_query(query: str) -> Dict[str, Any]:
    payload = query.encode("utf-8")
    last_err = None
    for base_url in OVERPASS_ENDPOINTS:
        # small backoff loop per mirror
        for attempt, backoff in enumerate([0.5, 1.5, 3.0], start=1):
            try:
                r = requests.post(
                    base_url,
                    data=payload,
                    headers=USER_AGENT,
                    timeout=(10, 180)  # connect 10s, read 180s
                )
                # Overpass can respond 429/504 during load
                if r.status_code >= 500 or r.status_code == 429:
                    last_err = HTTPException(status_code=r.status_code, detail=f"Overpass error {r.status_code} at {base_url}")
                    time.sleep(backoff)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_err = e
                time.sleep(backoff)
        # try next mirror
    # all mirrors failed
    if isinstance(last_err, HTTPException):
        raise last_err
    raise HTTPException(status_code=504, detail=f"All Overpass mirrors failed: {last_err}")

@app.post("/overpass/query")
def overpass_query(raw: str = Body(..., media_type="text/plain")) -> Dict[str, Any]:
    query = (raw or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="Overpass query must not be empty")
    return _overpass_query(query)

def overpass_to_geojson(opj: Dict[str, Any]) -> Dict[str, Any]:
    nodes: Dict[int, Tuple[float,float]] = {}
    ways: Dict[int, Dict[str, Any]] = {}
    for el in opj.get("elements", []):
        t = el.get("type")
        if t == "node": nodes[el["id"]] = (el["lon"], el["lat"])
        elif t == "way": ways[el["id"]] = el

    def way_to_ring(way_id: int):
        w = ways.get(way_id)
        if not w: return None
        coords = []
        for nd in w.get("nodes", []):
            if nd not in nodes: return None
            coords.append(nodes[nd])
        if len(coords) < 3: return None
        if coords[0] != coords[-1]: coords.append(coords[0])
        return coords, w.get("tags", {})

    PASSTHRU = [
        "name","building:use","amenity","shop","office","tourism","leisure","healthcare",
        "public_transport","railway","aeroway","landuse","man_made","operator","operator:type","brand","brand:wikidata",
        "ref","wikidata","wikipedia","source",
        "start_date","construction:year","opening_date","opening_hours",
        "addr:housenumber","addr:street","addr:city","addr:postcode","addr:country","addr:unit",
        "phone","email","website","contact:phone","contact:email","contact:website","contact:facebook","contact:instagram","contact:twitter",
        "access","wheelchair","wheelchair:description","entrance:wheelchair","toilets","toilets:wheelchair","tactile_paving","ramp","step_count","indoor","level",
        "bus","tram","subway","station"
    ]

    features: List[Dict[str, Any]] = []

    # ways
    for wid, w in ways.items():
        tags = w.get("tags", {})
        if "building" not in tags: continue
        ring = way_to_ring(wid)
        if ring is None: continue
        coords, tags = ring
        passthru = {k: tags[k] for k in PASSTHRU if k in tags}
        by_src = tags.get("start_date") or tags.get("construction:year")
        m = re.search(r"\b(\d{4})\b", str(by_src)) if by_src is not None else None
        built_year = int(m.group(1)) if m else None
        props = {
            "objekttyp": tags.get("building"),
            "height_m": _parse_height(tags),
            "built_year": built_year,
            "category": classify_building(tags),
            "src": "osm-way",
            **passthru
        }
        features.append({"type":"Feature","properties":props,"geometry":{"type":"Polygon","coordinates":[coords]}})

    # relations (multipolygons)
    for el in opj.get("elements", []):
        if el.get("type") != "relation": continue
        tags = el.get("tags", {})
        if "building" not in tags: continue
        outers: List[List[Tuple[float,float]]] = []
        inners: List[List[Tuple[float,float]]] = []
        for mem in el.get("members", []):
            if mem.get("type") != "way": continue
            ring = way_to_ring(mem["ref"])
            if ring is None: continue
            coords, _t2 = ring
            (inners if mem.get("role")=="inner" else outers).append(coords)
        if not outers: continue
        geom = {"type":"Polygon","coordinates":[outers[0]]+inners} if len(outers)==1 \
               else {"type":"MultiPolygon","coordinates":[[o] for o in outers]}
        passthru = {k: tags[k] for k in PASSTHRU if k in tags}
        by_src = tags.get("start_date") or tags.get("construction:year")
        m = re.search(r"\b(\d{4})\b", str(by_src)) if by_src is not None else None
        built_year = int(m.group(1)) if m else None
        props = {
            "objekttyp": tags.get("building"),
            "height_m": _parse_height(tags),
            "built_year": built_year,
            "category": classify_building(tags),
            "src": "osm-relation",
            **passthru
        }
        features.append({"type":"Feature","properties":props,"geometry":geom})

    return {"type":"FeatureCollection","features":features}

def new_job(city: str, years: List[int]) -> str:
    jid = uuid.uuid4().hex[:12]
    jdir = JOBS / jid
    jdir.mkdir(parents=True, exist_ok=True)
    (jdir / "request.json").write_text(json.dumps({"city": city, "years": years}, ensure_ascii=False, indent=2), encoding="utf-8")
    (jdir / "status.json").write_text(json.dumps({"status": "queued", "step": "queued"}), encoding="utf-8")
    return jid

def write_status(jid: str, status: str, step: str):
    (JOBS / jid / "status.json").write_text(json.dumps({"status": status, "step": step}), encoding="utf-8")

def job_dir(jid: str) -> Path:
    p = JOBS / jid
    if not p.exists(): raise HTTPException(status_code=404, detail="job not found")
    return p

def run_pipeline(jid: str):
    jdir = job_dir(jid)
    req = json.loads((jdir / "request.json").read_text(encoding="utf-8"))
    city = req["city"]

    try:
        write_status(jid, "running", "geocoding")
        g = geocode_city(city)
        south, west, north, east = g["bbox"]

        # modest pad
        pad_lat = (float(north) - float(south)) * 0.12
        pad_lon = (float(east) - float(west)) * 0.12
        bbox = (float(south)-pad_lat, float(west)-pad_lon, float(north)+pad_lat, float(east)+pad_lon)

        write_status(jid, "running", "overpass")
        op = overpass_buildings(bbox)

        write_status(jid, "running", "convert")
        fc = overpass_to_geojson(op)

        out_dir = STATIC_ROOT / "jobs" / jid
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "buildings.geojson").write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")

        write_status(jid, "complete", "complete")
    except Exception as e:
        write_status(jid, "failed", f"error: {e}")

@app.get("/health")
def health(): return {"ok": True}


@app.get("/", include_in_schema=False)
def root():
    if FRONTEND_INDEX.exists():
        return FileResponse(str(FRONTEND_INDEX))
    return {
        "ok": True,
        "service": "New Builds Explorer API",
        "health": "/health",
        "docs": "/docs",
    }


@app.get("/index.html", include_in_schema=False)
def index_html():
    if FRONTEND_INDEX.exists():
        return FileResponse(str(FRONTEND_INDEX))
    raise HTTPException(status_code=404, detail="frontend not found")


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "New Builds Explorer API",
        "health": "/health",
        "docs": "/docs",
    }

@app.post("/jobs")
def create_job(payload: Dict[str, Any]):
    city = (payload.get("city") or "").strip()
    years = payload.get("years") or []
    if not city: raise HTTPException(status_code=400, detail="missing city")
    jid = new_job(city, years)
    write_status(jid, "running", "start")
    run_pipeline(jid)  # inline for dev
    return {"job_id": jid}

@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    st = json.loads((JOBS / job_id / "status.json").read_text(encoding="utf-8"))
    return st

@app.get("/city/{job_id}/buildings")
def job_buildings(job_id: str):
    out_path = STATIC_ROOT / "jobs" / job_id / "buildings.geojson"
    if not out_path.exists(): raise HTTPException(status_code=404, detail="no artifact")
    return {"geojson": f"/static/jobs/{job_id}/buildings.geojson"}