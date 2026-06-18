import time
from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:5500/index.html"
OUT = "tools/_verify_access2sfca.png"

errors = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=[
        "--use-gl=angle", "--use-angle=swiftshader",
        "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist",
    ])
    page = browser.new_page(viewport={"width": 1500, "height": 950})
    page.on("console", lambda msg: errors.append(f"console.{msg.type}: {msg.text}") if msg.type in ("error", "warning") else None)
    page.on("pageerror", lambda exc: errors.append(f"PAGEERROR: {exc}"))

    page.goto(URL, wait_until="networkidle", timeout=60000)
    page.wait_for_selector('button[data-mode="walking"]', timeout=30000)

    # wait for buildings to load
    page.wait_for_function("() => (typeof baseCityFC!=='undefined') && baseCityFC?.features?.length > 0", timeout=40000)
    nb = page.evaluate("() => baseCityFC.features.length")
    print("BUILDINGS:", nb)

    # --- 1. runtime pipeline: load net + compute provision for several buildings ---
    res = page.evaluate("""async () => {
      const net = await ensureAccess2sfca('vaxjo','walking');
      if (!net) return {ok:false, why:'net null'};
      // sample 400 residential-ish buildings, compute overall provision
      const feats = baseCityFC.features;
      const step = Math.max(1, Math.floor(feats.length/400));
      let hit=0, sum=0, mx=0, mn=2, withCats=0;
      for (let i=0;i<feats.length;i+=step){
        const r = access2sfcaForFeature(feats[i]);
        if (!r) continue;
        hit++; sum+=r.overall; mx=Math.max(mx,r.overall); mn=Math.min(mn,r.overall);
        if (r.cats && Object.keys(r.cats).length) withCats++;
      }
      return {ok:true, city:net.city, mode:net.mode, cats:Object.keys(net.cats).length,
              sampled:hit, meanOverall:+(sum/Math.max(1,hit)).toFixed(4),
              minOverall:+mn.toFixed(4), maxOverall:+mx.toFixed(4), withCats};
    }""")
    print("PIPELINE:", res)

    # --- 2. drive the real inspector code path (same fn interaction.js calls) ---
    page.evaluate("""() => {
      // pick a fairly central building (closest to city meta center)
      const c=[14.8094,56.8787]; let best=null,bd=1e9;
      for (const f of baseCityFC.features){
        try{const g=turf.centroid(f).geometry.coordinates;
          const d=(g[0]-c[0])**2+(g[1]-c[1])**2; if(d<bd){bd=d;best=f;}}catch(e){}
      }
      window.__a2sTestFeat=best;
      window.faveInspector.setBuildingSelection(best);
    }""")
    # wait for async provision block to render
    page.wait_for_function(
        "() => (document.getElementById('inspSelectionList')||{}).textContent?.includes('2SFCA')",
        timeout=15000)
    block = page.evaluate("() => document.getElementById('inspSelectionList').textContent")
    print("INSPECTOR BLOCK contains 2SFCA:", "2SFCA" in block)
    print("INSPECTOR TEXT (trim):", " ".join(block.split())[:400])

    # --- 3. switch mode to driving and re-check (different catchments => values change) ---
    drv = page.evaluate("""async () => {
      const net = await ensureAccess2sfca('vaxjo','driving');
      const r = access2sfcaForFeature(window.__a2sTestFeat);
      return {mode:net?.mode, overall:r? +r.overall.toFixed(4):null,
              grocery: r?.cats?.grocery? +r.cats.grocery.norm.toFixed(3):null};
    }""")
    print("DRIVING central bldg:", drv)

    page.screenshot(path=OUT, full_page=False)
    print("SCREENSHOT:", OUT)
    browser.close()

print("---- CONSOLE/PAGE ERRORS ----")
for e in errors[:40]:
    print(e)
print("TOTAL ERR/WARN:", len(errors))
