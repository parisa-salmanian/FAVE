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

    # select a central building (highest pop / has fair_multi) via the inspector API
    picked = pg.evaluate("""()=>{
        const feats=(typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features:[];
        // pick a feature with fair_multi present near the city centre
        let best=null;
        for(const f of feats){ if(f.properties&&f.properties.fair_multi){ best=f; break; } }
        if(!best && feats.length) best=feats[Math.floor(feats.length/2)];
        if(!best) return {ok:false};
        window.faveInspector.setBuildingSelection(best);
        return {ok:true, name:best.properties&&(best.properties.name||best.properties.objekttyp||'')};
    }""")
    # wait for the async 2SFCA load to attach + render
    t0=time.time()
    while time.time()-t0<30:
        shown = pg.evaluate("()=> { const s=document.getElementById('insp2sfcaSection'); return s && s.style.display!=='none'; }")
        if shown: break
        time.sleep(0.5)
    time.sleep(1)

    info = pg.evaluate("""()=>{
        const sec=document.getElementById('insp2sfcaSection');
        const host=document.getElementById('insp2sfcaBars');
        const title=(document.getElementById('insp2sfcaTitle')||{}).textContent;
        const rows=[...host.querySelectorAll('.bar-row')].map(r=>({
            label:(r.querySelector('.label')||{}).innerText,
            num:(r.querySelector('.num')||{}).textContent,
            icon: !!r.querySelector('.label-icon'),
            width:(r.querySelector('.bar-fill')||{}).style ? r.querySelector('.bar-fill').style.width : null,
        }));
        const order = sec ? [...document.querySelectorAll('#inspCategoryBars, #insp2sfcaSection')].map(e=>e.id) : [];
        // confirm 2SFCA section sits AFTER the category bars in the DOM
        const catEl=document.getElementById('inspCategoryBars');
        const after = catEl && sec ? (catEl.compareDocumentPosition(sec) & Node.DOCUMENT_POSITION_FOLLOWING)!==0 : false;
        const hasPre = !!host.querySelector('pre');
        return { sectionShown: sec.style.display!=='none', title, nRows: rows.length,
                 rows: rows.slice(0,12), domOrderAfterCats: after, usesOldPre: hasPre };
    }""")
    pg.screenshot(path="tools/_verify_inspector2sfca.png")
    print("picked:", picked)
    print("section:", {k:info[k] for k in ('sectionShown','title','nRows','domOrderAfterCats','usesOldPre')})
    for r in info["rows"]:
        print("   row:", r)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
