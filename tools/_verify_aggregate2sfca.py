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
    t0=time.time()
    while time.time()-t0<45:
        g=pg.evaluate("()=> (document.getElementById('metricGini')||{}).textContent")
        if g and g.strip() not in ("","—"): break
        time.sleep(0.5)
    time.sleep(2)

    # preload 2SFCA network
    pg.evaluate("async()=>{ await ensureAccess2sfca('vaxjo','walking'); }")

    # ---- MACRO (district) ----
    pg.evaluate("()=>{ const b=document.getElementById('districtToggleBtn'); if(b) b.click(); }")
    t0=time.time()
    while time.time()-t0<30:
        ok=pg.evaluate("()=> typeof districtFC!=='undefined' && districtFC && districtFC.features && districtFC.features.some(f=>Number.isFinite(f.properties.__count)&&f.properties.__count>0)")
        if ok: break
        time.sleep(0.5)
    time.sleep(1)
    district = pg.evaluate("""async()=>{
        const f=districtFC.features.find(f=>Number.isFinite(f.properties.__count)&&f.properties.__count>20)||districtFC.features[0];
        // direct aggregation
        const rows=access2sfcaRowsInPolygon(f);
        const agg=access2sfcaAggregateForPolygon(f);
        // drive the inspector
        window.faveInspector.setDistrictSelection(f);
        return {name:f.properties.__districtName, count:f.properties.__count, rows:rows.length,
                aggN:agg&&agg.n, aggOverall:agg&&+agg.overall.toFixed(3)};
    }""")
    t0=time.time()
    while time.time()-t0<25:
        shown=pg.evaluate("()=>{const s=document.getElementById('insp2sfcaSection');return s&&s.style.display!=='none';}")
        if shown: break
        time.sleep(0.4)
    time.sleep(0.5)
    district_ui = pg.evaluate("""()=>{
        const host=document.getElementById('insp2sfcaBars');
        return {title:(document.getElementById('insp2sfcaTitle')||{}).textContent,
                rows:[...host.querySelectorAll('.bar-row')].slice(0,4).map(r=>({
                    label:(r.querySelector('.label')||{}).innerText.replace(/\\s+/g,' ').trim(),
                    num:(r.querySelector('.num')||{}).textContent}))};
    }""")

    # ---- MESO (hex) ----
    pg.evaluate("()=>{ const b=document.getElementById('mezoToggleBtn'); if(b) b.click(); }")
    t0=time.time()
    while time.time()-t0<30:
        ok=pg.evaluate("()=> typeof mezoHexData!=='undefined' && Array.isArray(mezoHexData) && mezoHexData.some(c=>c.__count>0)")
        if ok: break
        time.sleep(0.5)
    time.sleep(1)
    mezo = pg.evaluate("""async()=>{
        // pick the most central/accessible hex (highest fairness) with enough bldgs
        const cand=mezoHexData.filter(c=>c.__count>20&&Number.isFinite(c.__fairOverall));
        cand.sort((a,b)=>b.__fairOverall-a.__fairOverall);
        const c=cand[0]||mezoHexData.find(c=>c.__count>0)||mezoHexData[0];
        const rows=access2sfcaRowsInHex(c.hex);
        const agg=access2sfcaAggregateForHex(c.hex);
        window.faveInspector.setMezoSelection(c);
        return {hex:c.hex, count:c.__count, rows:rows.length, aggN:agg&&agg.n, aggOverall:agg&&+agg.overall.toFixed(3)};
    }""")
    t0=time.time()
    while time.time()-t0<25:
        shown=pg.evaluate("()=>{const s=document.getElementById('insp2sfcaSection');return s&&s.style.display!=='none';}")
        if shown: break
        time.sleep(0.4)
    time.sleep(0.5)
    mezo_ui = pg.evaluate("""()=>{
        const host=document.getElementById('insp2sfcaBars');
        return {title:(document.getElementById('insp2sfcaTitle')||{}).textContent,
                rows:[...host.querySelectorAll('.bar-row')].slice(0,4).map(r=>({
                    label:(r.querySelector('.label')||{}).innerText.replace(/\\s+/g,' ').trim(),
                    num:(r.querySelector('.num')||{}).textContent}))};
    }""")

    pg.screenshot(path="tools/_verify_aggregate2sfca.png")
    print("DISTRICT agg:", district)
    print("DISTRICT ui :", district_ui)
    print("MEZO agg:", mezo)
    print("MEZO ui :", mezo_ui)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
