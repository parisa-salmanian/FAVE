import time, json
from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:5500/index.html"
errors = []
with sync_playwright() as p:
    b = p.chromium.launch(headless=True, args=[
        "--use-gl=angle","--use-angle=swiftshader","--enable-unsafe-swiftshader","--ignore-gpu-blocklist"])
    pg = b.new_page(viewport={"width":1500,"height":950})
    pg.on("pageerror", lambda e: errors.append(f"PAGEERROR: {e}"))
    pg.on("console", lambda m: errors.append(f"console.error: {m.text}") if m.type=="error" else None)
    pg.goto(URL, wait_until="networkidle", timeout=60000)
    pg.wait_for_selector('button[data-mode="walking"]', timeout=30000)
    pg.click('#rail-poi'); pg.wait_for_selector('#shellPOIAll'); pg.click('#shellPOIAll')
    pg.keyboard.press("Escape")
    t0=time.time()
    while time.time()-t0<45:
        g=pg.evaluate("()=> (document.getElementById('metricGini')||{}).textContent")
        if g and g.strip() not in ("","—"): break
        time.sleep(0.5)
    time.sleep(2)

    res = pg.evaluate("""()=>{
        const r = computeGroupEquity();
        if(!r) return {ok:false};
        return {ok:true, city:r.city, nBuildings:r.nBuildings, totalPop:Math.round(r.totalPop),
                gini:+r.gini.toFixed(3), overallAccess:+r.overallAccess.toFixed(3),
                dims:r.dims.map(d=>({label:d.label, gap: d.gap==null?null:+(d.gap*100).toFixed(1),
                    atRisk:d.atRiskLabel,
                    groups:d.groups.map(g=>({label:g.label, mean:g.meanAccess==null?null:+(g.meanAccess*100).toFixed(0),
                                             share:+(g.share*100).toFixed(0)}))}))};
    }""")
    render = pg.evaluate("""()=>{
        renderGroupEquity();
        const host=document.getElementById('statsGroupEquity');
        return {rows:host.querySelectorAll('.bar-row').length, hasGini:host.innerHTML.includes('Gini'),
                len:host.innerHTML.length};
    }""")
    pg.screenshot(path="tools/_verify_equitygroups.png")
    print("computeGroupEquity:")
    print(json.dumps(res, indent=2))
    print("render:", render)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
