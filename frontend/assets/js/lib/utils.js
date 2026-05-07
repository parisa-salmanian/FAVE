// Small misc utilities (debounce, etc.). Extracted from main.js.

/* ======================= small utils ======================= */
function debounce(fn, ms=200) {
  let id=null;
  return (...args) => { if (id) clearTimeout(id); id=setTimeout(()=>fn(...args), ms); };
}
