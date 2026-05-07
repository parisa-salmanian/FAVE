// API client for the OSM-job pipeline. The osm_api source mode was removed
// (see CLAUDE.md "Hard rule") so this is currently dead code, kept here so any
// follow-up work can revive it via a baked workflow rather than rewriting.

/* ======================= API (OSM) helpers ======================= */
async function runCityJob(city='Växjö', years=[2021,2022,2023,2024], onStatus=()=>{}) {
  const post = await fetch(`${API_BASE}/jobs`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ city, years })
  });
  if (!post.ok) throw new Error(`Cannot create job: ${post.status} ${await post.text().catch(()=> '')}`);
  const { job_id } = await post.json();

  let status;
  do {
    onStatus('running');
    await new Promise(r => setTimeout(r, 650));
    const res = await fetch(`${API_BASE}/jobs/${job_id}`);
    if (!res.ok) throw new Error(`Status error ${res.status}`);
    status = await res.json();
    onStatus(status.step || status.status || 'running');
  } while (status.status !== 'complete' && status.status !== 'failed');

  if (status.status === 'failed') throw new Error(status.step || 'failed');
  const b = await fetch(`${API_BASE}/city/${job_id}/buildings`).then(r => r.json());
  const buildingsUrl = b.geojson.startsWith('/') ? `${API_BASE}${b.geojson}` : b.geojson;
  return { buildingsUrl };
}

