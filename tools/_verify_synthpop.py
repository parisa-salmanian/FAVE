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
    while time.time()-t0<60:
        g=pg.evaluate("()=> (document.getElementById('metricGini')||{}).textContent")
        if g and g.strip() not in ("","—"): break
        time.sleep(0.5)
    time.sleep(2)

    # 1) synthpop loaded + mapped?
    syn = pg.evaluate("""()=>({
        loaded: typeof SYNTHPOP!=='undefined' && !!SYNTHPOP,
        meta: (typeof SYNTHPOP!=='undefined'&&SYNTHPOP)?SYNTHPOP.meta:null,
        popMapSize: (typeof synthBuildingPopMap!=='undefined'&&synthBuildingPopMap)?synthBuildingPopMap.size:0,
        stamped: (typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features.filter(f=>f.properties&&f.properties.__synthPop>0).length:0,
        total: (typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features.length:0,
    })""")
    print("SYNTHPOP:", {k:syn[k] for k in ('loaded','popMapSize','stamped','total')})
    print("  meta:", syn["meta"])

    # 2) inspector shows synthetic fields for a building with synth pop
    picked = pg.evaluate("""()=>{
        const f=(baseCityFC.features||[]).find(x=>x.properties&&x.properties.__synthPop>0&&x.properties.fair_multi);
        if(!f) return {ok:false};
        window.faveInspector.setBuildingSelection(f);
        return {ok:true, synthPop:f.properties.__synthPop, levels:f.properties.__synthLevels,
                area:f.properties.__synthArea, zone:f.properties.__synthZone};
    }""")
    time.sleep(0.6)
    insp = pg.evaluate("""()=>{
        const kv=[...document.querySelectorAll('#inspSelectionList .kv')].map(e=>({
            k:(e.querySelector('.kv-key')||{}).textContent,
            v:(e.querySelector('.kv-val')||{}).textContent}));
        return kv;
    }""")
    print("picked building:", picked)
    print("inspector rows:", insp)
    synth_rows = [r for r in insp if "Synthetic" in (r["k"] or "") or "storeys" in (r["k"] or "").lower()
                  or "Footprint" in (r["k"] or "") or "zone" in (r["k"] or "").lower()]
    print("inspector synthetic rows shown:", synth_rows)

    # 3) PCP building dataset: synthetic axes present + vary per building (many distinct)
    pcp = pg.evaluate("""()=>{
        if(typeof getParallelCoordsDataset!=='function') return {err:'no fn'};
        const ds=getParallelCoordsDataset('building');
        const cats=ds.categories||[];
        const synAxes=cats.filter(c=>['synPop','synIncome','synNeed','synLevels','synArea','synZone'].includes(c));
        const distinct={};
        for(const a of ['synPop','synIncome','synNeed','demIncome','demNeed']){
            const s=new Set();
            ds.rows.forEach(r=>{const v=r.realValues&&r.realValues[a]; if(Number.isFinite(v)) s.add(Math.round(v*1000)/1000);});
            distinct[a]=s.size;
        }
        return {nRows:ds.rows.length, total:ds.total, synAxes, distinct};
    }""")
    print("PCP building:", pcp)

    print("ERRORS:", errors[:15], "total", len(errors))
    d = pcp.get("distinct",{}) if isinstance(pcp,dict) else {}
    income_row = [r for r in insp if "Income (synthetic)" in (r["k"] or "")]
    print("inspector income row:", income_row)
    print("PCP distinct — synIncome:%s vs demIncome:%s | synNeed:%s vs demNeed:%s"
          % (d.get("synIncome"), d.get("demIncome"), d.get("synNeed"), d.get("demNeed")))
    ok = (syn["popMapSize"]>0 and syn["stamped"]>0 and len(synth_rows)>=1
          and len(income_row)>=1
          and 'synIncome' in pcp.get("synAxes",[]) and 'synNeed' in pcp.get("synAxes",[])
          and d.get("synIncome",0) > d.get("demIncome",0)        # income now varies per-building
          and len(errors)==0)
    print("PASS:", ok)
    b.close()
