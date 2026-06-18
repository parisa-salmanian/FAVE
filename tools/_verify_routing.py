import time
from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:5500/index.html"
errors = []

def wait_metric(pg, timeout=45):
    t0 = time.time()
    while time.time() - t0 < timeout:
        g = pg.evaluate("()=>(document.getElementById('metricGini')||{}).textContent")
        if g and g.strip() not in ("", "—"): return
        time.sleep(0.5)

with sync_playwright() as p:
    b = p.chromium.launch(headless=True, args=[
        "--use-gl=angle","--use-angle=swiftshader","--enable-unsafe-swiftshader","--ignore-gpu-blocklist"])
    pg = b.new_page(viewport={"width":1500,"height":950})
    pg.on("pageerror", lambda e: errors.append(f"PAGEERROR: {e}"))
    pg.on("console", lambda m: errors.append(f"console.error: {m.text}") if m.type=="error" else None)
    pg.goto(URL, wait_until="networkidle", timeout=60000)
    pg.wait_for_selector('button[data-mode="driving"]', timeout=30000)
    pg.click('#rail-poi'); pg.wait_for_selector('#shellPOIAll'); pg.click('#shellPOIAll')
    pg.keyboard.press("Escape")
    wait_metric(pg); time.sleep(2)
    m_walk = pg.evaluate("()=>({gini:(document.getElementById('metricGini')||{}).textContent,overall:(document.getElementById('metricOverall')||{}).textContent})")

    pg.click('button[data-mode="driving"]'); time.sleep(4); wait_metric(pg); time.sleep(1)
    m_car = pg.evaluate("()=>({gini:(document.getElementById('metricGini')||{}).textContent,overall:(document.getElementById('metricOverall')||{}).textContent})")

    probe = pg.evaluate("""()=>{
        const ready = (typeof routingReady==='function') ? routingReady() : false;
        const hasCar = (typeof ROUTING!=='undefined'&&ROUTING)?{sig:ROUTING.sig,cats:Object.keys(ROUTING.cats).filter(c=>ROUTING.cats[c])}:null;
        // coverage: sample building centroids -> routingRowForPoint
        const feats=(typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features:[];
        let n=0,hit=0; const step=Math.max(1,Math.floor(feats.length/2000));
        let exampleNet=null, exampleStraight=null;
        for(let i=0;i<feats.length;i+=step){
            let c; try{c=turf.centroid(feats[i]).geometry.coordinates;}catch{continue;}
            const row=routingRowForPoint(c[0],c[1]); n++; if(row>=0)hit++;
            if(exampleNet===null && row>=0 && ROUTING.cats.grocery){
                const d=routingDistsForRow('grocery',row);
                if(d&&d.dists&&d.dists.length){
                    exampleNet=d.dists[0];
                    // straight-line nearest grocery from currentPOIsFC
                    const pois=(typeof currentPOIsFC!=='undefined'&&currentPOIsFC)?currentPOIsFC.features.filter(f=>f.properties&&f.properties.__cat==='grocery'):[];
                    let best=Infinity; const R=6371000,tr=x=>x*Math.PI/180;
                    for(const pf of pois){const g=pf.geometry.coordinates;const dLat=tr(g[1]-c[1]),dLon=tr(g[0]-c[0]);const s=Math.sin(dLat/2)**2+Math.cos(tr(c[1]))*Math.cos(tr(g[1]))*Math.sin(dLon/2)**2;best=Math.min(best,2*R*Math.asin(Math.sqrt(s)));}
                    exampleStraight=isFinite(best)?Math.round(best):null;
                }
            }
        }
        return {ready,hasCar,coveragePct:+(100*hit/n).toFixed(1),sampled:n,
                exampleNetGroceryM:exampleNet,exampleStraightGroceryM:exampleStraight};
    }""")
    pg.screenshot(path="tools/_verify_routing.png")
    print("WALK (network):", m_walk)
    print("CAR  (network):", m_car)
    print("PROBE:", probe)
    print("ERRORS:", errors[:15], "total", len(errors))
    b.close()
