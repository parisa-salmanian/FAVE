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

    toggle_present = pg.evaluate("()=> !!document.getElementById('priorityZonesToggle')")
    on = pg.evaluate("""()=>{
        setPriorityZones(true);
        const feats=(typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features:[];
        let scored=0, flagged=0, sampleColors=[];
        let pmin=Infinity, pmax=-Infinity;
        for(const f of feats){
            const P=priorityScoreForFeature(f);
            if(P!=null){ scored++; pmin=Math.min(pmin,P); pmax=Math.max(pmax,P);
                         if(P>=priorityZonesThreshold) flagged++; }
        }
        for(let i=0;i<feats.length && sampleColors.length<200;i+=Math.max(1,Math.floor(feats.length/200))){
            sampleColors.push(priorityColorForFeature(feats[i]).join(','));
        }
        return {active:priorityZonesOn(), scored, flagged, count:priorityZonesCount(),
                pMin:isFinite(pmin)?+pmin.toFixed(3):null, pMax:isFinite(pmax)?+pmax.toFixed(3):null,
                uniqueColors:new Set(sampleColors).size,
                legend:{left:(document.getElementById('spLegendLeft')||{}).textContent,
                        right:(document.getElementById('spLegendRight')||{}).textContent,
                        note:(document.getElementById('spNote')||{}).textContent}};
    }""")
    off = pg.evaluate("""()=>{
        setPriorityZones(false);
        return {active:priorityZonesOn(),
                legendRight:(document.getElementById('spLegendRight')||{}).textContent};
    }""")
    pg.screenshot(path="tools/_verify_priorityzones.png")
    print("toggle present:", toggle_present)
    print("ON :", on)
    print("OFF:", off)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
