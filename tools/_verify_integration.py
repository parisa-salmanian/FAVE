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

    # ---- 1) PCP BUILDING: no DESO income/need/pop axes; synthetic vary per-bldg ----
    pcb = pg.evaluate("""()=>{
        const ds=getParallelCoordsDataset('building'); const cats=ds.categories||[];
        const dead=cats.filter(c=>['demIncome','demNeed','demPop'].includes(c));
        const distinct=a=>{const s=new Set();ds.rows.forEach(r=>{const v=r.realValues&&r.realValues[a];if(Number.isFinite(v))s.add(Math.round(v*1000)/1000);});return s.size;};
        return {cats, deadAxes:dead, synIncomeDistinct:distinct('synIncome'), synPopDistinct:distinct('synPop')};
    }""")
    print("PCP building cats:", pcb["cats"])
    print("  dead DESO axes still present (should be []):", pcb["deadAxes"])
    print("  synIncome distinct:", pcb["synIncomeDistinct"], "| synPop distinct:", pcb["synPopDistinct"])

    # ---- 2) PCP MEZO: demographics now present with data ----
    pcm = pg.evaluate("""()=>{
        if(typeof mezoView==='undefined') {}
        const ds=getParallelCoordsDataset('mezo'); const cats=ds.categories||[];
        const demo=cats.filter(c=>['synPop','synIncome','synNeed','demChild','demElder'].includes(c));
        let withVals=0; ds.rows.forEach(r=>{ if(r.realValues&&Number.isFinite(r.realValues.synIncome)) withVals++; });
        return {nRows:ds.rows.length, demoAxes:demo, rowsWithIncome:withVals};
    }""")
    print("PCP mezo demoAxes:", pcm["demoAxes"], "| rows:", pcm["nRows"], "| rowsWithIncome:", pcm["rowsWithIncome"])

    # ---- 3) INSPECTOR demographics section below supply provision (building) ----
    pg.evaluate("""()=>{const f=(baseCityFC.features||[]).find(x=>x.properties&&x.properties.__synthPop>0&&x.properties.fair_multi);window.faveInspector.setBuildingSelection(f);}""")
    time.sleep(1.2)
    binsp = pg.evaluate("""()=>{
        const sec=document.getElementById('inspDemoSection');
        const a2s=document.getElementById('insp2sfcaSection');
        const after = (a2s&&sec)?(a2s.compareDocumentPosition(sec)&Node.DOCUMENT_POSITION_FOLLOWING)!==0:false;
        const rows=[...document.querySelectorAll('#inspDemoList .kv')].map(e=>(e.querySelector('.kv-key')||{}).textContent);
        return {shown: sec&&sec.style.display!=='none', belowSupply:after, rows};
    }""")
    print("INSPECTOR building demo — shown:", binsp["shown"], "belowSupply:", binsp["belowSupply"])
    print("   rows:", binsp["rows"])

    # ---- 4) SCROLL anchor: supply provision stays put when switching building ----
    pg.evaluate("""()=>{const sec=document.getElementById('insp2sfcaSection');if(sec)sec.scrollIntoView({block:'center'});}""")
    time.sleep(0.3)
    before = pg.evaluate("""()=>{const b=document.querySelector('#inspector .inspector-body');const s=document.getElementById('insp2sfcaSection');
        return s?Math.round(s.getBoundingClientRect().top - b.getBoundingClientRect().top):null;}""")
    # switch to a building with a long name (forces the title row to change height)
    pg.evaluate("""()=>{
        const feats=baseCityFC.features||[];
        let g=feats.find(x=>x.properties&&x.properties.__synthPop>0&&x.properties.fair_multi&&(x.properties.name||'').length>20)
              || feats.find(x=>x.properties&&x.properties.__synthPop>0&&x.properties.fair_multi);
        window.faveInspector.setBuildingSelection(g);
    }""")
    time.sleep(1.2)
    after = pg.evaluate("""()=>{const b=document.querySelector('#inspector .inspector-body');const s=document.getElementById('insp2sfcaSection');
        return s?Math.round(s.getBoundingClientRect().top - b.getBoundingClientRect().top):null;}""")
    drift = abs((before or 0)-(after or 0))
    print("SCROLL anchor — supply provision viewport-top before:", before, "after:", after, "drift:", drift, "px")

    print("ERRORS:", errors[:15], "total", len(errors))
    ok = (pcb["deadAxes"]==[] and pcb["synIncomeDistinct"]>200
          and len(pcm["demoAxes"])>=3 and pcm["rowsWithIncome"]>0
          and binsp["shown"] and binsp["belowSupply"] and len(binsp["rows"])>=5
          and drift<=10 and len(errors)==0)
    print("PASS:", ok)
    b.close()
