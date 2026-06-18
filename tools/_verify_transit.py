import sys, time
from playwright.sync_api import sync_playwright

URL = "http://127.0.0.1:5500/index.html"
OUT = "tools/_verify_transit.png"

def read_metrics(page):
    return page.evaluate("""() => ({
      metricGini: (document.getElementById('metricGini')||{}).textContent,
      metricOverall: (document.getElementById('metricOverall')||{}).textContent,
      giniOut: (document.getElementById('giniOut')||{}).textContent,
      overallGiniOut: (document.getElementById('overallGiniOut')||{}).textContent,
      activeMode: (document.querySelector('button[data-mode][data-active="true"]')||{}).dataset?.mode,
    })""")

def wait_metric(page, timeout=40):
    t0 = time.time()
    while time.time() - t0 < timeout:
        m = read_metrics(page)
        v = (m.get("metricGini") or "").strip()
        if v and v != "—":
            return m
        time.sleep(0.5)
    return read_metrics(page)

errors = []
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=[
        "--use-gl=angle", "--use-angle=swiftshader",
        "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist",
    ])
    page = browser.new_page(viewport={"width": 1500, "height": 950})
    page.on("console", lambda msg: errors.append(f"console.{msg.type}: {msg.text}") if msg.type in ("error","warning") else None)
    page.on("pageerror", lambda exc: errors.append(f"PAGEERROR: {exc}"))

    page.goto(URL, wait_until="networkidle", timeout=60000)
    # wait for the shell travel strip to render
    page.wait_for_selector('button[data-mode="walking"]', timeout=30000)
    page.wait_for_selector('button[data-mode="transit"]', timeout=30000)
    print("BUTTONS:", page.eval_on_selector_all('button[data-mode]', "els => els.map(e=>e.dataset.mode)"))

    # enable all POI categories to trigger a fairness compute
    page.click('#rail-poi', timeout=15000)
    page.wait_for_selector('#shellPOIAll', timeout=15000)
    page.click('#shellPOIAll', timeout=15000)
    time.sleep(1.0)
    # close popover
    page.keyboard.press("Escape")

    m_walk = wait_metric(page)
    print("WALK  :", m_walk)

    page.click('button[data-mode="driving"]', timeout=15000)
    time.sleep(3.0)
    m_car = wait_metric(page)
    print("CAR   :", m_car)

    page.click('button[data-mode="transit"]', timeout=15000)
    time.sleep(4.0)
    m_transit = wait_metric(page)
    print("TRANSIT:", m_transit)

    # confirm the transit network actually loaded
    net = page.evaluate("() => (typeof TRANSIT_NET!=='undefined' && TRANSIT_NET) ? {city:TRANSIT_NET.city, n:TRANSIT_NET.n} : null")
    print("TRANSIT_NET:", net)

    page.screenshot(path=OUT, full_page=False)
    print("SCREENSHOT:", OUT)
    browser.close()

print("---- CONSOLE/PAGE ERRORS ----")
for e in errors[:40]:
    print(e)
print("TOTAL ERR/WARN:", len(errors))
