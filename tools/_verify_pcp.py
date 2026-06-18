import time
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
    # wait for fairness compute
    t0=time.time()
    while time.time()-t0<45:
        g=pg.evaluate("()=> (document.getElementById('metricGini')||{}).textContent")
        if g and g.strip() not in ("","—"): break
        time.sleep(0.5)
    time.sleep(2)

    # open PCP drawer
    pg.click('#rail-parallel')
    time.sleep(2)
    panel_open = pg.evaluate("()=> !document.getElementById('parallelCoordsPanel').classList.contains('d-none')")

    def dataset(mode):
        return pg.evaluate("""(mode)=>{
            try{
                const d = getParallelCoordsDataset(mode);
                const demoCats = d.categories.filter(c=>/^dem/.test(c));
                const sample = d.rows[0] || null;
                const demoFilled = sample ? Object.keys(sample.realValues||{}).filter(k=>/^dem/.test(k)) : [];
                return {rows:d.rows.length, total:d.total, nCats:d.categories.length,
                        demoCats, demoFilledOnRow0:demoFilled, cats:d.categories.slice(0,30)};
            }catch(e){ return {error:String(e)}; }
        }""", mode)

    # building mode under each data source
    results = {}
    for src in ["mixed","poi","demo"]:
        pg.evaluate("(v)=>{ parallelCoordsDataSourceMode=v; const s=document.getElementById('parallelCoordsSource'); if(s)s.value=v; }", src)
        results[f"building/{src}"] = dataset("building")
    results["district/mixed"] = (pg.evaluate("()=>{parallelCoordsDataSourceMode='mixed';}"), dataset("district"))[1]

    # number of rendered polylines in the SVG
    lines = pg.evaluate("()=> document.querySelectorAll('#parallelCoordsPanel svg path').length")
    src_val = pg.evaluate("()=> (document.getElementById('parallelCoordsSource')||{}).value")
    pg.screenshot(path="tools/_verify_pcp.png")
    print("panel_open:", panel_open, "| selector value:", src_val, "| svg paths:", lines)
    for k,v in results.items():
        print(f"  {k}:", v)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
