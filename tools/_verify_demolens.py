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

    # selector populated?
    sel = pg.evaluate("""()=>{
        const s=document.getElementById('demoLensField');
        return s ? {opts:[...s.options].map(o=>o.value)} : null;
    }""")

    def lens_probe(field):
        return pg.evaluate("""(field)=>{
            setDemoLensField(field);
            const feats=(typeof baseCityFC!=='undefined'&&baseCityFC)?baseCityFC.features:[];
            let withDeso=0, colored=0, cols=[], norms=[];
            for(let i=0;i<feats.length && cols.length<400;i+=Math.max(1,Math.floor(feats.length/400))){
                const f=feats[i];
                if(f.properties&&f.properties.__deso!=null) withDeso++;
                const c=demoLensColorForFeature(f);
                const n=demoLensNormForFeature(f);
                if(Number.isFinite(n)){ colored++; norms.push(+n.toFixed(3)); }
                cols.push(c.join(','));
            }
            const uniqCols=new Set(cols).size;
            const leg={left:(document.getElementById('spLegendLeft')||{}).textContent,
                       mid:(document.getElementById('spLegendMid')||{}).textContent,
                       right:(document.getElementById('spLegendRight')||{}).textContent};
            return {active:demoLensActive(), field:demoLensField, sampled:cols.length,
                    withDeso, colored, uniqueColors:uniqCols,
                    normMin:norms.length?Math.min(...norms):null, normMax:norms.length?Math.max(...norms):null,
                    legend:leg};
        }""", field)

    res_elder = lens_probe("elder_frac")
    res_income = lens_probe("income")
    res_need = lens_probe("needZ")
    # turn off -> legend restored
    off = pg.evaluate("""()=>{
        setDemoLensField('');
        return {active:demoLensActive(),
                legendRight:(document.getElementById('spLegendRight')||{}).textContent,
                note:(document.getElementById('spNote')||{}).textContent};
    }""")
    pg.screenshot(path="tools/_verify_demolens.png")
    print("selector options:", sel)
    print("elder_frac:", res_elder)
    print("income    :", res_income)
    print("needZ     :", res_need)
    print("OFF       :", off)
    print("ERRORS:", errors[:20], "total", len(errors))
    b.close()
