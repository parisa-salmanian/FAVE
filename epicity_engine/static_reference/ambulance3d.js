/**
 * ambulance3d.js — In-scene 3D ambulance for active mobile clinics.
 *
 * One mesh per mobile_clinic spatial intervention. Each ambulance walks
 * the city's road network at bus-speed (~7 m/s × simSpeed), preferring
 * roads that bring it closer to the currently hottest building (highest
 * I) within range. At each road endpoint, it picks the connected road
 * whose far end is closest to the target — and reverses if it has no
 * connection at all.
 *
 * Coordinate space: planar metres (engine "world" coords), same as the
 * THREE.js scene already uses for buildings, roads, and transport
 * vehicles. The map → scene projection is identity.
 */

import * as THREE from 'three';
import { getScene } from './view.js';

let _roads        = [];        // [{ idx, points: [[x,z], ...], lengths: [...] }]
let _buildings    = [];        // city layout buildings (have world_x/z + lon/lat)
let _cells        = null;      // most recent /api/state cells (for I lookup)
// Pre-computed road GRAPH for proper navigation. Built once per city
// in setCityData() by bucketing road-polyline vertices onto a 4 m grid
// so OSM endpoints that are "almost the same intersection" collapse to
// a single graph node — exactly the trick the bus router uses in
// transport.py#_build_road_graph. The ambulance then walks node-to-
// node via Dijkstra to reach its waypoint instead of bouncing at the
// tolerance gap between adjacent road polylines.
let _graph        = null;      // { coords: [[x,z],…], adj: [[{to, d},…],…] }
const _ambulances = new Map(); // id → vehicle state
let _selectedId   = null;      // id of the ambulance with the yellow outline
// Uniform scale applied to the whole ambulance group on spawn. Buses use
// VEHICLE_SCALE = 2.5 in transport.js; we pick 2.2 so the ambulance reads
// slightly smaller than a bus while sharing the same scale family.
const AMB_SCALE = 2.2;
// Speed in m/s at simSpeed = 1, in WORLD metres (post-scale). Bus
// VEHICLE_BASE_SPEED is 45; ambulance is a touch slower so it feels
// purposeful but not frantic.
const _BASE_SPEED = 38.0;
const _SEEK_RADIUS_M = 3000.0;
const _MIN_WAYPOINT_GAP_M = 600;
// Effective post-scale half-dimensions in WORLD metres. Pre-scale mesh
// is 2.55 × 9.5; apply AMB_SCALE to match the rendered size.
const _AMB_MESH_HALF_W = 1.275 * AMB_SCALE;
const _AMB_MESH_HALF_L = 4.75  * AMB_SCALE;
// Slightly padded hit-test box around the mesh — friendlier picking
// near the edges. Same trick the bus uses.
const _AMB_HALF_W = _AMB_MESH_HALF_W + 0.6;
const _AMB_HALF_L = _AMB_MESH_HALF_L + 0.8;
let _meshTemplate = null;      // reusable geometry/material for cloning

/* ── City data wiring ─────────────────────────────────────────────────── */

/**
 * Capture the city layout once per city entry. Roads are pre-processed
 * (segment lengths cached) so per-frame movement only does cheap arithmetic.
 */
export function setCityData(layout) {
  _buildings = (layout && layout.buildings) || [];
  const raw  = (layout && layout.roads) || [];
  _roads = raw.map((r, idx) => {
    const pts = Array.isArray(r.points) ? r.points : [];
    const lens = [];
    for (let i = 1; i < pts.length; i++) {
      const dx = pts[i][0] - pts[i - 1][0];
      const dz = pts[i][1] - pts[i - 1][1];
      lens.push(Math.hypot(dx, dz));
    }
    return { idx, points: pts, lengths: lens };
  }).filter(r => r.points.length >= 2);

  _graph = _buildRoadGraph(_roads);
}

/* Mirrors transport.py#_build_road_graph — 4 m bucketing collapses
 * OSM endpoints that *should* be the same intersection (but are
 * stored a metre or two apart in the data) into a single node, so
 * Dijkstra can actually traverse the city without bouncing on
 * tolerance gaps. */
function _buildRoadGraph(roads) {
  const BUCKET = 4.0;
  const nodeIndex = new Map();
  const coords = [];
  const adj = [];
  const getNode = (x, z) => {
    const key = `${Math.round(x / BUCKET)},${Math.round(z / BUCKET)}`;
    const existing = nodeIndex.get(key);
    if (existing != null) return existing;
    const idx = coords.length;
    nodeIndex.set(key, idx);
    coords.push([x, z]);
    adj.push([]);
    return idx;
  };
  for (const r of roads) {
    const pts = r.points || [];
    if (pts.length < 2) continue;
    let prev = getNode(pts[0][0], pts[0][1]);
    for (let i = 1; i < pts.length; i++) {
      const cur = getNode(pts[i][0], pts[i][1]);
      if (cur === prev) continue;
      const cu = coords[prev], cv = coords[cur];
      const d  = Math.hypot(cv[0] - cu[0], cv[1] - cu[1]);
      adj[prev].push({ to: cur, d });
      adj[cur].push({ to: prev, d });
      prev = cur;
    }
  }
  return { coords, adj };
}

/** Nearest road-graph node to a world-XZ point (linear scan; the
 *  graph is small enough that a grid index isn't worth the complexity). */
function _nearestNode(x, z, maxDist = 400) {
  if (!_graph) return -1;
  const max2 = maxDist * maxDist;
  let best = -1, bestD2 = Infinity;
  for (let i = 0; i < _graph.coords.length; i++) {
    const dx = _graph.coords[i][0] - x;
    const dz = _graph.coords[i][1] - z;
    const d2 = dx * dx + dz * dz;
    if (d2 < bestD2) { bestD2 = d2; best = i; }
  }
  return bestD2 < max2 ? best : -1;
}

/** Dijkstra shortest path from src → dst node ids. Returns the node
 *  list (inclusive of both ends) or null when unreachable. Heap-less
 *  linear scan is fine for graphs of a few thousand nodes. */
function _dijkstra(src, dst) {
  if (!_graph) return null;
  if (src === dst) return [src];
  const n = _graph.coords.length;
  const dist = new Float64Array(n).fill(Infinity);
  const prev = new Int32Array(n).fill(-1);
  const done = new Uint8Array(n);
  dist[src] = 0;
  while (true) {
    // Pick unvisited node with smallest tentative distance.
    let u = -1, ud = Infinity;
    for (let i = 0; i < n; i++) if (!done[i] && dist[i] < ud) { ud = dist[i]; u = i; }
    if (u === -1) break;
    if (u === dst) break;
    done[u] = 1;
    for (const e of _graph.adj[u]) {
      const nd = ud + e.d;
      if (nd < dist[e.to]) { dist[e.to] = nd; prev[e.to] = u; }
    }
  }
  if (dist[dst] === Infinity) return null;
  const path = [];
  for (let u = dst; u !== -1; u = prev[u]) path.unshift(u);
  return path;
}

/** Forward the latest cells so the ambulance can pick hot targets. */
export function setCells(cells) { _cells = cells || null; }

/* ── Spawn / despawn ──────────────────────────────────────────────────── */

/** Spawn (or re-position) an ambulance for clinic `id` near (lon, lat). */
export function spawn(id, lon, lat) {
  if (_ambulances.has(id)) return;          // idempotent
  const scene = getScene();
  if (!scene || _roads.length === 0) return;

  // Find spawn world coords by snapping to the nearest building's centre.
  const spawnWorld = _nearestBuildingWorld(lon, lat);
  if (!spawnWorld) return;

  // Find the nearest road segment to the spawn point.
  const seed = _nearestSegment(spawnWorld[0], spawnWorld[1]);
  if (!seed) return;

  const mesh = _buildAmbulanceMesh();
  // Uniform scale so the ambulance reads at the same family as the
  // buses (which apply VEHICLE_SCALE on geometry); kept slightly under
  // bus scale so the two vehicles are visually distinguishable.
  mesh.scale.set(AMB_SCALE, AMB_SCALE, AMB_SCALE);
  scene.add(mesh);

  _ambulances.set(id, {
    id,
    mesh,
    outline:  null,
    roadIdx:  seed.roadIdx,
    segIdx:   seed.segIdx,
    segT:     seed.segT,
    dir:      +1,
    spawnWorld,
    spawnLonlat: [lon, lat],
    bridge:   null,    // legacy smooth-crossing state (graph-mode path supersedes)
    path:     null,    // list of graph node ids
    pathPos:  0,       // index of current edge start within `path`
    edgeT:    0,       // 0..1 progress along current edge
    // Up to 3 hot waypoints we're cycling between. Refreshed on a
    // 10-second cadence so the ambulance keeps tracking the moving
    // outbreak instead of fixating on one early hot building.
    waypoints:        [],
    waypointIdx:      0,
    waypointTtlSec:   0,
  });
}

/** Remove an ambulance — clinic was dismissed or expired. */
export function despawn(id) {
  const a = _ambulances.get(id);
  if (!a) return;
  const scene = getScene();
  if (scene) scene.remove(a.mesh);
  _disposeMesh(a.mesh);
  _ambulances.delete(id);
}

/** Drop every ambulance — called on city leave / reset. */
export function clearAll() {
  for (const id of [..._ambulances.keys()]) despawn(id);
  _selectedId = null;
}

/* ── Selection: yellow ground ring + tooltip integration ─────────────── */

// Match the bus selection ring (transport.js _buildSelectionOutline):
// a flat yellow rectangle drawn at ground level beneath the vehicle,
// oriented with its heading, pulsing in opacity each frame. Lives as
// a single shared mesh inside the scene — we just move it onto the
// selected ambulance.
let _selectionRing  = null;

function _ensureSelectionRing() {
  if (_selectionRing) return _selectionRing;
  const scene = getScene();
  if (!scene) return null;
  // Match the ambulance's true mesh footprint — no extra margin. The
  // hit-test box is padded separately (_AMB_HALF_*).
  const hl = _AMB_MESH_HALF_L;
  const hw = _AMB_MESH_HALF_W;
  const pts = [
    new THREE.Vector3( hw, 0, -hl),
    new THREE.Vector3( hw, 0,  hl),
    new THREE.Vector3(-hw, 0,  hl),
    new THREE.Vector3(-hw, 0, -hl),
    new THREE.Vector3( hw, 0, -hl),
  ];
  const geo = new THREE.BufferGeometry().setFromPoints(pts);
  const mat = new THREE.LineBasicMaterial({
    color: 0xfbbf24, transparent: true, opacity: 0.95, depthTest: false,
  });
  _selectionRing = new THREE.Line(geo, mat);
  _selectionRing.renderOrder = 1001;
  _selectionRing.visible = false;
  scene.add(_selectionRing);
  return _selectionRing;
}

/** Mark an ambulance as selected. Pass null to clear. */
export function setSelected(id) {
  if (_selectedId === id) return;
  _selectedId = id || null;
  const ring = _ensureSelectionRing();
  if (ring) ring.visible = !!_selectedId;
}

export function getSelected() { return _selectedId; }

/** Update the ring position + heading + pulse every frame. */
function _updateSelectionRing() {
  if (!_selectionRing || !_selectionRing.visible || !_selectedId) return;
  const a = _ambulances.get(_selectedId);
  if (!a) { _selectionRing.visible = false; return; }
  _selectionRing.position.set(a.mesh.position.x, 0.05, a.mesh.position.z);
  _selectionRing.rotation.y = a.mesh.rotation.y;
  // Pulsing alpha — same beat as the bus ring.
  const phase = 0.5 + 0.5 * Math.sin(Date.now() / 260);
  _selectionRing.material.opacity = 0.55 + 0.45 * phase;
}

/**
 * Cursor hit-test against the ambulances. Same projection idea as
 * `transport.hitTest`: unproject NDC into a world-space ray, intersect
 * with each ambulance's Y-plane (y ≈ 1.6), then test in the vehicle's
 * local oriented bounding box. Returns the ambulance id or null.
 */
export function hitTest(mouseX, mouseY, mvpMatrix, rect) {
  if (_ambulances.size === 0 || !mvpMatrix || !rect) return null;
  const ndcX =  ((mouseX - rect.left) / rect.width)  * 2 - 1;
  const ndcY = -((mouseY - rect.top)  / rect.height) * 2 + 1;
  const inv  = mvpMatrix.clone().invert();
  const near = new THREE.Vector3(ndcX, ndcY, -1).applyMatrix4(inv);
  const far  = new THREE.Vector3(ndcX, ndcY,  1).applyMatrix4(inv);
  const dir  = new THREE.Vector3().subVectors(far, near).normalize();

  let bestId = null, bestD2 = Infinity;
  for (const a of _ambulances.values()) {
    const x = a.mesh.position.x;
    const z = a.mesh.position.z;
    const y = a.mesh.position.y;
    // Ray-plane intersection at y
    if (Math.abs(dir.y) < 1e-6) continue;
    const t = (y - near.y) / dir.y;
    if (t < 0) continue;
    const hx = near.x + t * dir.x;
    const hz = near.z + t * dir.z;
    // Local axes: forward is the +Z of the mesh after rotation around Y.
    const rotY = a.mesh.rotation.y;
    const fwdX = Math.sin(rotY), fwdZ = Math.cos(rotY);
    const rgtX = -fwdZ,          rgtZ =  fwdX;
    const dx = hx - x, dz = hz - z;
    const along = dx * fwdX + dz * fwdZ;
    const across = dx * rgtX + dz * rgtZ;
    if (Math.abs(along) > _AMB_HALF_L) continue;
    if (Math.abs(across) > _AMB_HALF_W) continue;
    const d2 = dx * dx + dz * dz;
    if (d2 < bestD2) { bestD2 = d2; bestId = a.id; }
  }
  return bestId;
}

/** Lookup the source spatial item for this ambulance (label, days, …). */
export function getInfo(id) {
  const a = _ambulances.get(id);
  if (!a) return null;
  // Resolve "from" (nearest building right now) and "to" (current
  // waypoint's nearest building) into human-readable names so the
  // tooltip can show a real route line.
  const fromBld = _nearestBuilding([a.mesh.position.x, a.mesh.position.z]);
  const target  = _currentWaypoint(a);
  const toBld   = target ? _nearestBuilding(target) : null;
  // Lon/lat for the ambulance's *current* location — handy for the
  // active-measures camera fly-to so it tracks the ambulance instead
  // of zooming to where the user originally placed the clinic.
  const currentLonlat = fromBld && fromBld.lon != null
    ? [fromBld.lon, fromBld.lat]
    : (a.spawnLonlat || null);
  return {
    id,
    lonlat:        a.spawnLonlat || null,
    currentLonlat,
    worldPos:      [a.mesh.position.x, a.mesh.position.z],
    from:          fromBld ? (fromBld.name || fromBld.neighborhood || null) : null,
    to:            toBld   ? (toBld.name   || toBld.neighborhood   || null) : null,
  };
}

/** Project the selected ambulance's world position to viewport pixels.
 *  Mirrors transport.getVehicleScreenPos — used by the ui.js tooltip
 *  follower so the popup tracks the moving ambulance. */
export function getScreenPos(id, mvpMatrix, rect) {
  const a = _ambulances.get(id);
  if (!a || !mvpMatrix || !rect) return null;
  const wx = a.mesh.position.x;
  const wy = a.mesh.position.y + 2.5;   // a bit above the body so popup floats over it
  const wz = a.mesh.position.z;
  const v = new THREE.Vector4(wx, wy, wz, 1).applyMatrix4(mvpMatrix);
  if (v.w <= 0) return null;
  const ndcX = v.x / v.w;
  const ndcY = v.y / v.w;
  const sx = rect.left + (ndcX *  0.5 + 0.5) * rect.width;
  const sy = rect.top  + (-ndcY * 0.5 + 0.5) * rect.height;
  return { sx, sy };
}

function _nearestBuilding(worldXZ) {
  if (!_buildings || _buildings.length === 0) return null;
  let best = null, bestD2 = Infinity;
  // Strided scan — same trick as in _topHotNearby.
  for (let i = 0; i < _buildings.length; i += 19) {
    const b = _buildings[i];
    if (!b || b.world_x == null) continue;
    const dx = b.world_x - worldXZ[0];
    const dz = b.world_z - worldXZ[1];
    const d2 = dx * dx + dz * dz;
    if (d2 < bestD2) {
      bestD2 = d2;
      best = b;
    }
  }
  return best;
}

/** Sync the live set of ambulances against an items list. */
export function syncFromItems(items) {
  const live = new Set();
  for (const it of items || []) {
    if (it.type !== 'mobile_clinic') continue;
    if (!it.center_lonlat || it.center_lonlat.length !== 2) continue;
    live.add(it.id);
    if (_ambulances.has(it.id)) continue;
    // Before spawning a new mesh, see if there's an existing optimistic
    // ambulance (id prefixed `__opt_`) at roughly the same spawn point.
    // If yes, migrate its id to the canonical one — the mesh stays put
    // and the user sees no blink when the server response lands.
    const optId = _findOptimisticAt(it.center_lonlat);
    if (optId) {
      const a = _ambulances.get(optId);
      _ambulances.delete(optId);
      a.id = it.id;
      _ambulances.set(it.id, a);
      // Carry over the selection ring if the user happened to click the
      // optimistic ambulance before the server confirmed.
      if (_selectedId === optId) _selectedId = it.id;
    } else {
      spawn(it.id, it.center_lonlat[0], it.center_lonlat[1]);
    }
  }
  for (const id of [..._ambulances.keys()]) {
    if (!live.has(id)) despawn(id);
  }
}

function _findOptimisticAt(lonlat) {
  // Match within ~30 m so floating-point round-trips through JSON can't
  // miss the optimistic entry. There's at most a handful of ambulances
  // at any time so the linear scan is cheap.
  for (const [id, a] of _ambulances) {
    if (!id.startsWith('__opt_')) continue;
    if (!a.spawnLonlat) continue;
    const dlon = (a.spawnLonlat[0] - lonlat[0]) * 111000
               * Math.cos((a.spawnLonlat[1] + lonlat[1]) / 2 * Math.PI / 180);
    const dlat = (a.spawnLonlat[1] - lonlat[1]) * 111000;
    if (Math.hypot(dlon, dlat) < 30) return id;
  }
  return null;
}

/* ── Per-frame walk ───────────────────────────────────────────────────── */

/**
 * Advance every ambulance one frame. Called from ui.js's animation loop
 * alongside transport.update().
 *
 * @param {number} dt        seconds since last frame
 * @param {number} simSpeed  user's simulation speed slider value (≥ 0)
 */
export function update(dt, simSpeed) {
  if (_ambulances.size === 0 || _roads.length === 0) return;
  // Honour the play/pause state — when paused (simSpeed === 0), the
  // ambulance freezes in place, same as buses. Waypoint refresh also
  // stops so we don't accumulate stale TTL while the user is paused.
  // Always update the selection ring so the yellow outline tracks the
  // ambulance position even while paused (the user can still inspect it).
  _updateSelectionRing();
  if (!(simSpeed > 0)) return;
  // Cap dt so a stalled / backgrounded browser tab can't dump 1+ second
  // of "missed time" into a single walk step. With base speed 38 × max(1,
  // simSpeed), a 1 s dt at 2× would teleport the ambulance ~76 m in one
  // frame — exactly the "jumping from one side to the other" the user
  // sees after a tab switch. Capping at 50 ms keeps frame steps bounded.
  const cappedDt = Math.min(0.05, Math.max(0, dt));
  const speed = _BASE_SPEED * Math.max(1, simSpeed) * cappedDt;
  for (const a of _ambulances.values()) {
    _refreshWaypoints(a, cappedDt);
    _walk(a, speed);
  }
}

/** Pick (or refresh) up to 3 hot waypoints the ambulance cycles between. */
function _refreshWaypoints(a, dt) {
  a.waypointTtlSec -= dt;
  if (a.waypoints.length > 0 && a.waypointTtlSec > 0) return;

  // Top-N highest-I buildings within range, skipping any that's too
  // close to one already chosen so waypoints span real distance and the
  // ambulance has long routes to traverse rather than hopping between
  // adjacent neighbours of the same outbreak.
  // Search around the ambulance's CURRENT position so new outbreaks
  // that appear in front of it become candidates — anchoring to the
  // spawn point would keep the ambulance chasing only the first
  // generation of infected buildings forever.
  const here = [a.mesh.position.x, a.mesh.position.z];
  const all = _topHotNearby(here, 16);
  const picked = [];
  for (const cand of all) {
    let ok = true;
    for (const p of picked) {
      if (Math.hypot(p[0] - cand[0], p[1] - cand[1]) < _MIN_WAYPOINT_GAP_M) {
        ok = false; break;
      }
    }
    if (ok) picked.push(cand);
    if (picked.length >= 4) break;
  }
  a.waypoints = picked;
  a.waypointIdx = 0;
  a.waypointTtlSec = 8;
  // Recompute the road-graph path so the ambulance heads toward the
  // freshest infection target instead of finishing a stale route.
  _refreshPath(a);   // refresh cadence — short enough that the
                          // ambulance picks up newly-infected buildings
                          // promptly as the epidemic spreads, long
                          // enough that it commits to a route.
}

function _currentWaypoint(a) {
  if (!a.waypoints.length) return null;
  return a.waypoints[a.waypointIdx % a.waypoints.length];
}

function _advanceWaypoint(a) {
  if (a.waypoints.length) a.waypointIdx = (a.waypointIdx + 1) % a.waypoints.length;
}

/** Compute (or recompute) the road-graph path from where the ambulance
 *  currently is to its current waypoint. If the current waypoint is
 *  unreachable, advance to the next one and try again — never leave
 *  the ambulance with an empty path so it keeps moving. */
function _refreshPath(a) {
  if (!_graph || _graph.coords.length === 0) return;
  let here;
  if (a.path && a.pathPos < a.path.length) {
    here = a.path[a.pathPos];
  } else {
    here = _nearestNode(a.mesh.position.x, a.mesh.position.z);
  }
  if (here < 0) return;
  // Try up to N waypoints — discard any unreachable from the current node.
  for (let attempts = 0; attempts < Math.max(1, a.waypoints.length); attempts++) {
    const wp = _currentWaypoint(a);
    if (!wp) break;
    const dst = _nearestNode(wp[0], wp[1]);
    if (dst < 0 || dst === here) { _advanceWaypoint(a); continue; }
    const path = _dijkstra(here, dst);
    if (path && path.length >= 2) {
      a.path    = path;
      a.pathPos = 0;
      a.edgeT   = 0;
      return;
    }
    _advanceWaypoint(a);
  }
  // No reachable waypoint — wander to a random connected neighbour so
  // the ambulance keeps moving until fresh waypoints arrive.
  const neighbours = _graph.adj[here];
  if (neighbours && neighbours.length > 0) {
    const pick = neighbours[Math.floor(Math.random() * neighbours.length)].to;
    a.path    = [here, pick];
    a.pathPos = 0;
    a.edgeT   = 0;
  }
}

// Walk + path-following are now graph-driven. If a `path` is available
// the ambulance follows it node-by-node, computed by Dijkstra to its
// current waypoint. Falls back to the old polyline walk for cities
// where the graph couldn't be built (defensive).
function _walk(a, dist) {
  // Graph-mode walk: prefer this when we have a path through the
  // pre-computed road graph. The path is recomputed whenever the
  // ambulance reaches its end or the waypoint changes.
  if (_graph && _graph.coords.length > 0) {
    if (!a.path || a.path.length < 2) _refreshPath(a);
    if (!a.path || a.path.length < 2) return;

    let remaining = dist;
    while (remaining > 0 && a.pathPos < a.path.length - 1) {
      const u = a.path[a.pathPos];
      const v = a.path[a.pathPos + 1];
      const cu = _graph.coords[u], cv = _graph.coords[v];
      const edgeLen = Math.hypot(cv[0] - cu[0], cv[1] - cu[1]) || 0.01;
      const room = (1 - a.edgeT) * edgeLen;
      if (remaining <= room) {
        a.edgeT += remaining / edgeLen;
        remaining = 0;
      } else {
        remaining -= room;
        a.edgeT = 0;
        a.pathPos++;
      }
    }
    // Reached the destination — pick a new waypoint and recompute.
    if (a.pathPos >= a.path.length - 1) {
      _advanceWaypoint(a);
      _refreshPath(a);
    }
    // Position + heading along the current edge.
    if (a.path && a.pathPos < a.path.length - 1) {
      const u = a.path[a.pathPos];
      const v = a.path[a.pathPos + 1];
      const cu = _graph.coords[u], cv = _graph.coords[v];
      const t  = Math.max(0, Math.min(1, a.edgeT));
      a.mesh.position.set(
        cu[0] + (cv[0] - cu[0]) * t,
        1.6,
        cu[1] + (cv[1] - cu[1]) * t,
      );
      a.mesh.rotation.y = Math.atan2(cv[0] - cu[0], cv[1] - cu[1]);
    }
    return;
  }

  // Legacy fallback (kept for cities without a built graph).
  let road = _roads[a.roadIdx];
  if (!road) return;
  let remaining = dist;

  // If a previous frame ended mid-bridge (gap between road endpoints
  // at a junction), continue walking the bridge first. The ambulance
  // appears to drive straight across the intersection instead of
  // snapping to the next road's endpoint.
  if (a.bridge) {
    while (a.bridge && remaining > 0) {
      const total = a.bridge.total;
      const need  = (1 - a.bridge.progress) * total;
      if (remaining <= need) {
        a.bridge.progress += remaining / total;
        remaining = 0;
      } else {
        remaining -= need;
        a.bridge.progress = 1;
      }
      if (a.bridge.progress >= 1) {
        // Bridge done — adopt the destination road state.
        a.roadIdx = a.bridge.toRoadIdx;
        road = _roads[a.roadIdx];
        if (a.bridge.fromStart) {
          a.segIdx = 0; a.segT = 0; a.dir = +1;
        } else {
          a.segIdx = road.lengths.length - 1; a.segT = 1; a.dir = -1;
        }
        a.bridge = null;
      }
    }
    if (a.bridge) {
      // Still bridging — position is along the straight-line bridge.
      const b = a.bridge;
      const tx = b.from[0] + (b.to[0] - b.from[0]) * b.progress;
      const tz = b.from[1] + (b.to[1] - b.from[1]) * b.progress;
      const dx = b.to[0] - b.from[0];
      const dz = b.to[1] - b.from[1];
      a.mesh.position.set(tx, 1.6, tz);
      a.mesh.rotation.y = Math.atan2(dx, dz);
      return;
    }
  }

  while (remaining > 0) {
    const segLen = road.lengths[a.segIdx] || 0;
    if (segLen <= 0.01) {
      if (a.dir > 0) a.segIdx++; else a.segIdx--;
    } else {
      const room = a.dir > 0
        ? (1 - a.segT) * segLen
        : (a.segT) * segLen;
      if (remaining <= room) {
        a.segT += (remaining / segLen) * a.dir;
        remaining = 0;
      } else {
        remaining -= room;
        a.segT = a.dir > 0 ? 1 : 0;
        if (a.dir > 0) a.segIdx++; else a.segIdx--;
      }
    }

    // Reached an endpoint of the current road?
    if (a.segIdx >= road.lengths.length || a.segIdx < 0) {
      const atEndPoint = a.segIdx >= road.lengths.length
        ? road.points[road.points.length - 1]
        : road.points[0];
      // If we're close to the current waypoint, advance to the next.
      const wp = _currentWaypoint(a);
      if (wp && Math.hypot(atEndPoint[0] - wp[0], atEndPoint[1] - wp[1]) < 80) {
        _advanceWaypoint(a);
      }
      const target = _currentWaypoint(a);
      const next = _pickNextRoad(road.idx, atEndPoint, target);
      if (!next) {
        // Bounce — no connected road.
        if (a.segIdx >= road.lengths.length) {
          a.segIdx = road.lengths.length - 1; a.segT = 1;
        } else {
          a.segIdx = 0; a.segT = 0;
        }
        a.dir = -a.dir;
      } else {
        // Compute the bridge: straight-line walk from the current
        // endpoint to the new road's connecting endpoint. If the gap
        // is negligible (<1 m), just snap; otherwise spend movement
        // budget walking across it so the user sees a smooth turn
        // rather than a 25 m teleport at the intersection.
        const newRoad = _roads[next.roadIdx];
        const newEnd  = next.fromStart
          ? newRoad.points[0]
          : newRoad.points[newRoad.points.length - 1];
        const gap = Math.hypot(newEnd[0] - atEndPoint[0],
                               newEnd[1] - atEndPoint[1]);
        if (gap < 1.0) {
          // Snap is invisible at this distance.
          road = newRoad;
          a.roadIdx = next.roadIdx;
          if (next.fromStart) { a.segIdx = 0; a.segT = 0; a.dir = +1; }
          else { a.segIdx = newRoad.lengths.length - 1; a.segT = 1; a.dir = -1; }
        } else {
          // Start a smooth bridge and consume what we can this frame.
          a.bridge = {
            from: [atEndPoint[0], atEndPoint[1]],
            to:   [newEnd[0],     newEnd[1]],
            total: gap,
            progress: 0,
            toRoadIdx: next.roadIdx,
            fromStart: next.fromStart,
          };
          // Recurse the same frame so any remaining budget continues
          // through the bridge.
          if (remaining > 0) {
            const need = gap;
            if (remaining <= need) {
              a.bridge.progress = remaining / gap;
              remaining = 0;
            } else {
              remaining -= need;
              a.bridge.progress = 1;
              // Bridge complete this frame — adopt destination state
              // and keep walking the rest of remaining on the new road.
              a.roadIdx = next.roadIdx;
              road = newRoad;
              if (next.fromStart) { a.segIdx = 0; a.segT = 0; a.dir = +1; }
              else { a.segIdx = newRoad.lengths.length - 1; a.segT = 1; a.dir = -1; }
              a.bridge = null;
            }
          }
          // If we still have a bridge in progress, break — position
          // will be computed from bridge state below.
          if (a.bridge) break;
        }
      }
    }
  }

  // If we're mid-bridge at the end of this frame, use bridge position.
  if (a.bridge) {
    const b = a.bridge;
    const tx = b.from[0] + (b.to[0] - b.from[0]) * b.progress;
    const tz = b.from[1] + (b.to[1] - b.from[1]) * b.progress;
    const dx = b.to[0] - b.from[0];
    const dz = b.to[1] - b.from[1];
    a.mesh.position.set(tx, 1.6, tz);
    a.mesh.rotation.y = Math.atan2(dx, dz);
    return;
  }

  // Position + rotation on the segment.
  const pts = road.points;
  const i   = Math.max(0, Math.min(pts.length - 2, a.segIdx));
  const p0  = pts[i];
  const p1  = pts[i + 1];
  const t   = Math.max(0, Math.min(1, a.segT));
  const x   = p0[0] + (p1[0] - p0[0]) * t;
  const z   = p0[1] + (p1[1] - p0[1]) * t;
  const dx  = (p1[0] - p0[0]) * a.dir;
  const dz  = (p1[1] - p0[1]) * a.dir;
  a.mesh.position.set(x, 1.6, z);
  a.mesh.rotation.y = Math.atan2(dx, dz);
}

/* ── Road-graph helpers (built lazily, no precomputation) ─────────────── */

// Endpoints within this radius are treated as a shared junction.
// OSM-imported road segments at the same intersection often have
// endpoints 15-30 m apart, so a tight tolerance (5 m) leaves the
// ambulance stuck on its starting road — no junctions to hop through.
// We use 25 m to catch real junctions and SMOOTHLY BRIDGE the gap on
// hop instead of snapping (see `a.bridge` state in `_walk`), so the
// transition looks like the ambulance crossing an intersection
// rather than teleporting between road endpoints.
const _CONN_TOL = 25.0;

function _pickNextRoad(currentIdx, atPoint, target) {
  // Among roads (other than current) whose first or last point sits within
  // _CONN_TOL of `atPoint`, pick the one whose far endpoint is closest to
  // `target`. If no target, fall back to a random connected road. We
  // ALSO add a small random component to the score so the ambulance
  // doesn't always pick the exact same road and look stuck in a loop.
  let best = null, bestScore = Infinity;
  for (let k = 0; k < _roads.length; k++) {
    if (k === currentIdx) continue;
    const r = _roads[k];
    const head = r.points[0];
    const tail = r.points[r.points.length - 1];
    let fromStart = null;
    if (Math.hypot(head[0] - atPoint[0], head[1] - atPoint[1]) < _CONN_TOL) fromStart = true;
    else if (Math.hypot(tail[0] - atPoint[0], tail[1] - atPoint[1]) < _CONN_TOL) fromStart = false;
    if (fromStart === null) continue;
    const farEnd = fromStart ? tail : head;
    let score;
    if (target) {
      score = Math.hypot(farEnd[0] - target[0], farEnd[1] - target[1])
            + Math.random() * 30;
    } else {
      score = Math.random() * 1000;
    }
    if (score < bestScore) {
      bestScore = score;
      best = { roadIdx: k, fromStart };
    }
  }
  return best;
}

/** Top-N highest-I buildings within _SEEK_RADIUS_M of (x, z), in world coords. */
function _topHotNearby(point, n = 6) {
  if (!_cells || _buildings.length === 0) return [];
  const r2 = _SEEK_RADIUS_M * _SEEK_RADIUS_M;
  const candidates = [];
  for (let i = 0; i < _buildings.length; i += 7) {
    const b = _buildings[i];
    if (!b || b.world_x == null) continue;
    const dx = b.world_x - point[0];
    const dz = b.world_z - point[1];
    const d2 = dx * dx + dz * dz;
    if (d2 > r2) continue;
    const c = _cells[b.idx];
    const I = c ? (c.I || 0) : 0;
    // Even mildly infected buildings count — the user wants the
    // ambulance to chase the whole infection-flow chain, not only the
    // hottest cluster centres.
    if (I < 0.1) continue;
    candidates.push([b.world_x, b.world_z, I]);
  }
  // Sort by I desc; trim to n.
  candidates.sort((a, b) => b[2] - a[2]);
  return candidates.slice(0, n).map(c => [c[0], c[1]]);
}

function _nearestBuildingWorld(lon, lat) {
  let best = null, bestD2 = Infinity;
  for (const b of _buildings) {
    if (b.lon == null || b.lat == null) continue;
    const dlon = (b.lon - lon) * 111000;
    const dlat = (b.lat - lat) * 111000;
    const d2 = dlon * dlon + dlat * dlat;
    if (d2 < bestD2) {
      bestD2 = d2;
      best = [b.world_x, b.world_z];
    }
  }
  return best;
}

function _nearestSegment(x, z) {
  let bestRoadIdx = -1, bestSegIdx = 0, bestSegT = 0, bestD2 = Infinity;
  for (let r = 0; r < _roads.length; r++) {
    const pts = _roads[r].points;
    for (let s = 0; s < pts.length - 1; s++) {
      const ax = pts[s][0],     az = pts[s][1];
      const bx = pts[s + 1][0], bz = pts[s + 1][1];
      const segDx = bx - ax, segDz = bz - az;
      const segL2 = segDx * segDx + segDz * segDz;
      if (segL2 < 1) continue;
      let t = ((x - ax) * segDx + (z - az) * segDz) / segL2;
      t = Math.max(0, Math.min(1, t));
      const cx = ax + t * segDx, cz = az + t * segDz;
      const d2 = (cx - x) * (cx - x) + (cz - z) * (cz - z);
      if (d2 < bestD2) {
        bestD2     = d2;
        bestRoadIdx = r;
        bestSegIdx  = s;
        bestSegT    = t;
      }
    }
  }
  if (bestRoadIdx < 0) return null;
  return { roadIdx: bestRoadIdx, segIdx: bestSegIdx, segT: bestSegT };
}

/* ── Mesh (red ambulance, bus-shaped, with wheels + doors) ──────────────
 * Recipe mirrors the bus in transport.js: rounded body, dual rear axle,
 * sliding side doors, rear loading doors, wheel arches, side mirrors,
 * windshield + porthole windows, plus a roof beacon and red cross. */

// Match the bus size from transport.js (bus is L=10, W=3). Scaled to
// 95% so it reads as a slightly compact emergency van but at bus scale.
const AMB_LEN  = 9.5;
const AMB_HALF = AMB_LEN / 2;
const AMB_BODY_W = 2.85;

const _matBody   = new THREE.MeshLambertMaterial({ color: 0xef4444 });   // red
const _matWhite  = new THREE.MeshLambertMaterial({ color: 0xffffff });   // white panels / roof
const _matCross  = new THREE.MeshBasicMaterial   ({ color: 0xef4444 });   // medical cross
const _matWin    = new THREE.MeshBasicMaterial   ({ color: 0x0f172a });   // dark glass
const _matWheel  = new THREE.MeshLambertMaterial({ color: 0x111827 });   // tire black
const _matHub    = new THREE.MeshLambertMaterial({ color: 0x9ca3af });   // hubcap silver
const _matBumper = new THREE.MeshLambertMaterial({ color: 0x374151 });   // dark grey
const _matLight  = new THREE.MeshBasicMaterial   ({ color: 0xfef3c7 });   // headlight cream
const _matTail   = new THREE.MeshBasicMaterial   ({ color: 0xdc2626 });   // tail-light red
const _matBeacon = new THREE.MeshBasicMaterial   ({ color: 0xef4444, transparent: true, opacity: 0.9 });

function _buildAmbulanceMesh() {
  const g = new THREE.Group();
  // Never let the mesh be culled by the frustum — each child's bounding
  // sphere is computed from its local geometry, not the group, so a
  // tight camera angle can drop pieces of the body intermittently and
  // make the ambulance "blink" between frames. The cost of always
  // rendering one extra vehicle is negligible.
  g.frustumCulled = false;
  const halfL = AMB_HALF;

  // ── Lower skirt (darker trim, sits beneath the body) ──
  g.add(_box(2.55, 0.30, AMB_LEN,  0, 0.15, 0,                _matBumper));

  // ── Main body (red box, ambulance proportions) ──
  const body = _box(2.5, 1.6, AMB_LEN, 0, 1.10, 0, _matBody);
  body.castShadow = true; g.add(body);

  // White cab cap above the windshield
  g.add(_box(2.4, 1.2, 2.0,  0, 2.50, halfL - 1.0,   _matWhite));
  // Box-style ambulance back (slightly taller than cab) — kept as the
  // same-height main body for simplicity; visual cue is the white roof.
  g.add(_box(2.4, 1.2, AMB_LEN - 2.2,  0, 2.50, -1.1, _matWhite));

  // ── Roof + medical cross ──
  g.add(_box(2.5, 0.2, AMB_LEN - 0.2,  0, 3.20, 0,    _matWhite));
  g.add(_box(2.0, 0.05, 0.6,           0, 3.32, 0,    _matCross));
  g.add(_box(0.6, 0.05, 2.0,           0, 3.32, 0,    _matCross));

  // Roof beacons — pulsing red domes on either side of the cross
  for (const dx of [-1.0, 1.0]) {
    const dome = new THREE.Mesh(new THREE.SphereGeometry(0.18, 10, 6), _matBeacon);
    dome.position.set(dx, 3.45, 0.5);
    g.add(dome);
  }

  // ── Windshield + side windows (dark glass) ──
  g.add(_box(2.20, 1.15, 0.10,  0,    2.50, halfL + 0.02, _matWin));
  for (const dx of [-1.27, 1.27]) {
    g.add(_box(0.06, 0.55, 1.6,  dx, 2.65, halfL - 1.8, _matWin));   // front-cab side window
  }
  // Small side porthole windows in the patient compartment
  for (const dx of [-1.27, 1.27]) {
    for (const dz of [0.5, -1.5]) {
      g.add(_box(0.04, 0.45, 0.60, dx, 2.55, dz, _matWin));
    }
  }

  // ── Front face (bumper, headlights, grille) ──
  g.add(_box(2.4, 0.8, 0.15, 0, 1.30, halfL,         _matBumper));
  for (const dx of [-0.85, 0.85]) {
    const hl = new THREE.CylinderGeometry(0.20, 0.20, 0.15, 12);
    hl.rotateX(Math.PI / 2);
    hl.translate(dx, 1.0, halfL + 0.08);
    g.add(new THREE.Mesh(hl, _matLight));
  }
  g.add(_box(2.5, 0.35, 0.25, 0, 0.45, halfL + 0.05, _matBumper));   // front bumper

  // ── Rear face (loading doors + tail lights + rear bumper) ──
  // Two rear loading doors (fairly tall, with thin window strip)
  for (const dx of [-0.6, 0.6]) {
    g.add(_box(1.10, 1.85, 0.05, dx, 1.95, -halfL - 0.025, _matWhite));
    g.add(_box(1.05, 0.30, 0.04, dx, 2.55, -halfL - 0.045, _matWin));   // door window strip
  }
  // Door handles
  for (const dx of [-0.30, 0.30]) {
    g.add(_box(0.18, 0.05, 0.05, dx, 1.85, -halfL - 0.05, _matBumper));
  }
  // Tail lights
  for (const dx of [-0.95, 0.95]) {
    g.add(_box(0.30, 0.20, 0.10, dx, 1.60, -halfL - 0.06, _matTail));
  }
  // Rear bumper
  g.add(_box(2.5, 0.35, 0.25, 0, 0.45, -halfL - 0.05, _matBumper));

  // ── Sliding side door (right side, between front cab and rear) ──
  for (const dx of [1.27, -1.27]) {
    g.add(_box(0.06, 1.6, 1.4, dx, 1.85, halfL - 3.6, _matWhite));
    // Vertical seam between sliding door and body
    g.add(_box(0.07, 1.6, 0.04, dx, 1.85, halfL - 4.3, _matBumper));
    // Handle
    g.add(_box(0.04, 0.05, 0.20, dx + (dx > 0 ? 0.02 : -0.02), 1.95, halfL - 4.0, _matBumper));
  }

  // ── Wheels: front pair + dual rear axle (mirrors the bus) ──
  for (const dx of [-1.3, 1.3]) {
    const wf = new THREE.CylinderGeometry(0.55, 0.55, 0.40, 14);
    wf.rotateZ(Math.PI / 2);
    wf.translate(dx, 0.55, halfL - 1.8);
    g.add(new THREE.Mesh(wf, _matWheel));
    const hub = new THREE.CylinderGeometry(0.25, 0.25, 0.42, 12);
    hub.rotateZ(Math.PI / 2);
    hub.translate(dx, 0.55, halfL - 1.8);
    g.add(new THREE.Mesh(hub, _matHub));
  }
  for (const dx of [-1.2, 1.2]) {
    for (const mult of [0.85, 1.15]) {
      const w = new THREE.CylinderGeometry(0.55, 0.55, 0.25, 14);
      w.rotateZ(Math.PI / 2);
      w.translate(dx * mult, 0.55, -halfL + 1.8);
      g.add(new THREE.Mesh(w, _matWheel));
    }
  }
  // Wheel arches — half-cylinders behind the wheels
  for (const dz of [halfL - 1.8, -halfL + 1.8]) {
    const arch = new THREE.CylinderGeometry(0.72, 0.72, 2.6, 14, 1, false, 0, Math.PI);
    arch.rotateZ(Math.PI / 2);
    arch.translate(0, 0.55, dz);
    g.add(new THREE.Mesh(arch, _matBumper));
  }

  // ── Side mirrors (small protrusions on the cab) ──
  for (const dx of [-1.40, 1.40]) {
    g.add(_box(0.35, 0.06, 0.30, dx, 2.65, halfL - 0.20, _matBumper));
    g.add(_box(0.20, 0.20, 0.12, dx, 2.60, halfL,        _matWin));
  }

  // Frustum culling propagates per-mesh in THREE.js — setting it on the
  // Group alone doesn't disable culling for the children. Walk every
  // mesh and turn it off so no body / wheel / door pops out of view
  // at an unlucky camera angle.
  g.traverse(o => { if (o.isMesh) o.frustumCulled = false; });
  return g;
}

function _box(w, h, d, x, y, z, mat) {
  const m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), mat);
  m.position.set(x, y, z);
  return m;
}

function _disposeMesh(group) {
  // Materials are module-shared — only geometries are per-instance.
  group.traverse(o => {
    if (o.isMesh) o.geometry?.dispose?.();
  });
}
