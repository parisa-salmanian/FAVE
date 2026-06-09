#!/usr/bin/env bash
# One-time OSRM preprocessing for FAVE's three travel profiles.
#
# Downloads the Sweden OSM extract once and prepares it for walking, cycling,
# and driving using the OSRM MLD pipeline (extract -> partition -> customize).
# After this finishes, run `docker compose up -d` in this directory to serve
# the three routing servers locally, then run tools/bake_routing.py.
#
# This is a DEV step. It hits the internet (Geofabrik) to download the extract,
# which is allowed — the runtime app never does. See RUNBOOK.md.
#
# Requirements: Docker Desktop installed and running. ~2 GB download, several
# GB of working files, and 10-40 min of CPU depending on your machine.
#
# Usage:
#   ./setup_osrm.sh                 # all three profiles
#   ./setup_osrm.sh foot            # just walking
#   PBF_URL=... ./setup_osrm.sh     # override the extract (e.g. a smaller region)

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$HERE/data"
IMAGE="osrm/osrm-backend:latest"
PBF_URL="${PBF_URL:-https://download.geofabrik.de/europe/sweden-latest.osm.pbf}"
PBF_NAME="sweden-latest.osm.pbf"

# profile key -> OSRM lua profile shipped inside the image (/opt/<name>.lua).
# Plain case statement (no associative arrays) so this runs on macOS bash 3.2.
profile_lua() {
  case "$1" in
    foot) echo "foot" ;;
    bike) echo "bicycle" ;;
    car)  echo "car" ;;
    *)    echo "" ;;
  esac
}

if [ "$#" -eq 0 ]; then
  PROFILES="foot bike car"
else
  PROFILES="$*"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: Docker is not installed or not on PATH. Install Docker Desktop first." >&2
  exit 1
fi

mkdir -p "$DATA_DIR"

# 1) Download the Sweden extract once (shared by all profiles).
SHARED_PBF="$DATA_DIR/$PBF_NAME"
if [ ! -f "$SHARED_PBF" ]; then
  echo ">> Downloading $PBF_URL"
  curl -L --fail -o "$SHARED_PBF" "$PBF_URL"
else
  echo ">> Reusing existing $SHARED_PBF"
fi

# 2) Prepare each profile in its own subdir (OSRM writes .osrm* next to the pbf).
for key in $PROFILES; do
  lua="$(profile_lua "$key")"
  if [ -z "$lua" ]; then
    echo "ERROR: unknown profile '$key' (use: foot bike car)" >&2
    exit 2
  fi

  pdir="$DATA_DIR/$key"
  mkdir -p "$pdir"
  cp -f "$SHARED_PBF" "$pdir/$PBF_NAME"

  echo ">> [$key] osrm-extract (profile: $lua)"
  docker run --rm -t -v "$pdir:/data" "$IMAGE" \
    osrm-extract -p "/opt/$lua.lua" "/data/$PBF_NAME"

  echo ">> [$key] osrm-partition"
  docker run --rm -t -v "$pdir:/data" "$IMAGE" \
    osrm-partition "/data/sweden-latest.osrm"

  echo ">> [$key] osrm-customize"
  docker run --rm -t -v "$pdir:/data" "$IMAGE" \
    osrm-customize "/data/sweden-latest.osrm"

  # The pbf copy is no longer needed once the .osrm files exist.
  rm -f "$pdir/$PBF_NAME"
  echo ">> [$key] ready."
done

echo
echo "All requested profiles prepared. Next:"
echo "  cd \"$HERE\" && docker compose up -d"
echo "  # then bake: .venv/bin/python tools/bake_routing.py"
