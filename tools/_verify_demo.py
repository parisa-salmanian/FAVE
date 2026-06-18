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
    # wait for compute
    t0=time.time()
    while time.time()-t0<45:
        g=pg.evaluate("()=> (document.getElementById('metricGini')||{}).textContent")
        if g and g.strip() not in ("","—"): break
        time.sleep(0.5)
    time.sleep(2)
    metrics = pg.evaluate("""()=>({
        gini:(document.getElementById('metricGini')||{}).textContent,
        overall:(document.getElementById('metricOverall')||{}).textContent})""")
    demo = pg.evaluate("""()=>{
        const epi = (typeof EPI_DEMO!=='undefined'&&EPI_DEMO)?{city:EPI_DEMO.city,desos:EPI_DEMO.features.length}:null;
        const needSize = (typeof epiBuildingNeedMap!=='undefined'&&epiBuildingNeedMap)?epiBuildingNeedMap.size:0;
        const popSize  = (typeof epiBuildingPopMap!=='undefined'&&epiBuildingPopMap)?epiBuildingPopMap.size:0;
        // sample socio weights from computed buildings
        let samples=[], min=Infinity, max=-Infinity, varied=0;
        const feats=(typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features:[];
        for(let i=0;i<feats.length && samples.length<5;i+=Math.max(1,Math.floor(feats.length/50))){
            const ic=feats[i].properties&&feats[i].properties.__ifcity;
            if(ic&&Number.isFinite(ic.socio_weight)){samples.push({sw:+ic.socio_weight.toFixed(3),sz:ic.socio_z});}
        }
        for(const f of feats){const ic=f.properties&&f.properties.__ifcity; if(ic&&Number.isFinite(ic.socio_weight)){min=Math.min(min,ic.socio_weight);max=Math.max(max,ic.socio_weight); if(Math.abs(ic.socio_weight-1)>1e-6)varied++;}}
        // demand weight distribution
        let dmin=Infinity,dmax=-Infinity,dvar=0;
        for(const f of feats){const ic=f.properties&&f.properties.__ifcity; if(ic&&Number.isFinite(ic.demand_weight)){dmin=Math.min(dmin,ic.demand_weight);dmax=Math.max(dmax,ic.demand_weight); if(Math.abs(ic.demand_weight-1)>1e-6)dvar++;}}
        const vaxPop=(typeof vaxjoBuildingPopMap!=='undefined'&&vaxjoBuildingPopMap)?vaxjoBuildingPopMap.size:0;
        return {epi,needSize,popSize,vaxPop,samples,swMin:isFinite(min)?+min.toFixed(3):null,swMax:isFinite(max)?+max.toFixed(3):null,buildingsWithVariedSocio:varied,demandMin:isFinite(dmin)?+dmin.toFixed(3):null,demandMax:isFinite(dmax)?+dmax.toFixed(3):null,buildingsWithVariedDemand:dvar};
    }""")
    pg.screenshot(path="tools/_verify_demo.png")
    print("WALKING metrics (WITH demographics):", metrics)
    print("DEMO state:", demo)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
