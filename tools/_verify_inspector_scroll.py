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

    # Click building A, wait for its 2SFCA card, scroll the body down to the card.
    pg.evaluate("""()=>{
        const f=(baseCityFC.features||[]).find(x=>x.properties&&x.properties.fair_multi);
        window.__A=f; window.faveInspector.setBuildingSelection(f);
    }""")
    t0=time.time()
    while time.time()-t0<30:
        if pg.evaluate("()=>{const s=document.getElementById('insp2sfcaSection');return s&&s.style.display!=='none';}"): break
        time.sleep(0.5)
    time.sleep(1)
    # scroll body so the supply card is in view
    pg.evaluate("""()=>{const sec=document.getElementById('insp2sfcaSection');
        sec.scrollIntoView({block:'end'});}""")
    time.sleep(0.4)
    before = pg.evaluate("""()=>{const body=document.querySelector('#inspector .inspector-body');
        const sec=document.getElementById('insp2sfcaSection');
        return {scrollTop:Math.round(body.scrollTop), shown:sec.style.display!=='none'};}""")

    # Simulate a HOVER onto a different building B (what handleBuildingHover does):
    # new selection with a2s=null first, then the async attach. Capture scrollTop
    # at the moment the card is mid-load and again after it settles.
    pg.evaluate("""()=>{
        const feats=baseCityFC.features||[];
        let g=feats.find(x=>x.properties&&x.properties.fair_multi&&x!==window.__A);
        window.faveInspector.setBuildingSelection(g);
    }""")
    mid = pg.evaluate("""()=>{const body=document.querySelector('#inspector .inspector-body');
        const sec=document.getElementById('insp2sfcaSection');
        return {scrollTop:Math.round(body.scrollTop), shown:sec.style.display!=='none',
                loading:sec.getAttribute('data-loading')};}""")
    time.sleep(1.0)
    after = pg.evaluate("""()=>{const body=document.querySelector('#inspector .inspector-body');
        const sec=document.getElementById('insp2sfcaSection');
        return {scrollTop:Math.round(body.scrollTop), shown:sec.style.display!=='none',
                loading:sec.getAttribute('data-loading')};}""")

    print("before hover:", before)
    print("mid  (loading):", mid)
    print("after settle :", after)
    drift = abs(before["scrollTop"] - after["scrollTop"])
    print("scroll drift before->after:", drift, "px")
    print("card stayed visible through hover:", before["shown"] and mid["shown"] and after["shown"])
    print("PASS:", before["shown"] and mid["shown"] and after["shown"] and drift <= 8 and before["scrollTop"] > 0)
    print("ERRORS:", errors[:10], "total", len(errors))
    b.close()
