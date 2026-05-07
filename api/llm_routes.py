from __future__ import annotations

"""FastAPI routes for Llama-powered natural language assistance."""

import json
import os
import re
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/llm", tags=["llm"])

# Local Ollama configuration
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")

# Optional Hugging Face fallback (kept for UI plan generation compatibility)
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "meta-llama/Meta-Llama-3-8B-Instruct")
HF_API_URL = os.getenv(
    "HUGGINGFACE_API_URL",
    f"https://api-inference.huggingface.co/models/{LLAMA_MODEL}",
)
HF_TOKEN = os.getenv("HUGGINGFACE_TOKEN")

# Safe defaults for generation – we want concise, deterministic JSON
HF_PARAMS: Dict[str, Any] = {
    "max_new_tokens": 256,
    "temperature": 0.2,
    "return_full_text": False,
}

# Whitelisted actions the frontend knows how to dispatch
ACTION_SCHEMA = {
    "LOAD_CITY": "{\"type\":\"LOAD_CITY\",\"city\":<city string>}",
    "SET_SOURCE": "{\"type\":\"SET_SOURCE\",\"source\":\"osm|s1\"}",
    "SET_MODE": "{\"type\":\"SET_MODE\",\"mode\":\"all|new\"}",
    "SET_YEAR": "{\"type\":\"SET_YEAR\",\"year\":<integer year>}",
    "SET_HEIGHT": "{\"type\":\"SET_HEIGHT\",\"scale\":<float scale>}",
    "SET_FAIRNESS_TRAVEL_MODE": "{\"type\":\"SET_FAIRNESS_TRAVEL_MODE\",\"mode\":\"walking|driving|cycling\"}",
    "SET_POI": "{\"type\":\"SET_POI\",\"category\":<poi category>}",
    "SET_POI_MIX": "{\"type\":\"SET_POI_MIX\",\"categories\":[<poi category>],\"weights\":{<poi category>:<weight>}}",
    "SET_POI_SYMBOLS": "{\"type\":\"SET_POI_SYMBOLS\",\"enabled\":<true|false>}",
    "COMPUTE_FAIRNESS": "{\"type\":\"COMPUTE_FAIRNESS\"}",
    "ROUTE_BETWEEN": "{\"type\":\"ROUTE_BETWEEN\",\"from\":<name>,\"to\":<name>}",
    "WHATIF_SUGGEST": "{\"type\":\"WHATIF_SUGGEST\",\"prompt\":<user request>,\"categories\":[<poi category>]}",
    "WHATIF_LASSO_APPLY": "{\"type\":\"WHATIF_LASSO_APPLY\",\"counts\":[{\"type\":<building type>,\"count\":<integer>}]}",
    "SET_DISTRICT_VIEW": "{\"type\":\"SET_DISTRICT_VIEW\",\"enabled\":<true|false>}",
    "SHOW_DISTRICT_STATS": "{\"type\":\"SHOW_DISTRICT_STATS\",\"district\":<district name>}",
    "EXPLAIN": "{\"type\":\"EXPLAIN\",\"message\":<short note>}",
}


class PlanRequest(BaseModel):
    question: str
    context: Optional[Dict[str, Any]] = None


class PlanResponse(BaseModel):
    actions: List[Dict[str, Any]]


class ExplainRequest(BaseModel):
    view: str
    summary: Dict[str, Any]
    question: Optional[str] = None


class ExplainResponse(BaseModel):
    text: str

class WhatIfIntentRequest(BaseModel):
    question: str
    context: Optional[Dict[str, Any]] = None


class WhatIfIntentResponse(BaseModel):
    categories: List[str]
    count: int = 1
    mode: Optional[str] = None
    focus: Optional[str] = None
    fairness_target: Optional[str] = None
    area: Optional[str] = None
    rationale: Optional[str] = None


PROMPT_TEMPLATE = """
You are a planner for a web GIS application that explores cities and fairness metrics.
The user asks a question and you must return JSON describing UI actions.
Respond with a compact JSON object that follows this TypeScript type:

  interface Plan {{ actions: Array<Action> }}
  type Action =
    | {{type:'LOAD_CITY', city:string}}
    | {{type:'SET_SOURCE', source:'osm'|'s1'}}
    | {{type:'SET_MODE', mode:'all'|'new'}}
    | {{type:'SET_YEAR', year:number}}
    | {{type:'SET_HEIGHT', scale:number}}
    | {{type:'SET_FAIRNESS_TRAVEL_MODE', mode:'walking'|'driving'|'cycling'}}
    | {{type:'SET_POI', category:string}}
    | {{type:'SET_POI_MIX', categories:string[], weights?: Record<string, number>}}
    | {{type:'SET_POI_SYMBOLS', enabled:boolean}}
    | {{type:'COMPUTE_FAIRNESS'}}
    | {{type:'ROUTE_BETWEEN', from:string, to:string}}
    | {{type:'WHATIF_SUGGEST', prompt:string, categories:string[]}}
    | {{type:'WHATIF_LASSO_APPLY', counts:Array<{type:string, count:number}>}}
    | {{type:'SET_DISTRICT_VIEW', enabled:boolean}}
    | {{type:'SHOW_DISTRICT_STATS', district:string}}
    | {{type:'EXPLAIN', message:string}}

Rules:
- Output ONLY JSON (no Markdown, no explanations).
- Prefer a short sequence of actions to satisfy the request.
- If the user asks about multiple POI categories, use SET_POI_MIX with a list of categories.
- If the user provides weights for POI categories, include a weights map in SET_POI_MIX.
- If the user asks to hide/remove symbols, use SET_POI_SYMBOLS with enabled=false.
- If the user asks to show/enable symbols, use SET_POI_SYMBOLS with enabled=true.
- If the user specifies a travel distance mode (walking, driving, cycling), use SET_FAIRNESS_TRAVEL_MODE.
- If the user asks where/how many POIs to add for fairness, use WHATIF_SUGGEST.
- If the user asks to add buildings to a selected/lasso area, use WHATIF_LASSO_APPLY.
- If the user asks to show districts, use SET_DISTRICT_VIEW.
- If the user asks for district statistics, use SHOW_DISTRICT_STATS.
- When unsure, include an EXPLAIN action.
- Do not hallucinate cities outside the context; default to the provided city when missing.

Return a JSON object with double-quoted keys/strings, for example:
{{"actions":[{{"type":"LOAD_CITY","city":"Gothenburg"}}]}}

Context:
{context}

User question:
{question}
"""


EXPLAIN_TEMPLATE = """
You are an on-screen analyst helping a user explore city-level fairness metrics in a dashboard.
The dashboard contains a DR scatterplot with lasso, fairness histograms, Lorenz curve, fairness-by-category bars, threshold bars, and contrastive distribution stats.
Given a view name, a compact JSON summary, and an optional user question, write a concise interpretation.

Instructions:
- Keep the answer under 120 words and use 2-4 short sentences.
- Reference the provided view and statistics directly; avoid speculation.
- If the question is missing, focus on describing the salient patterns and any notable inequality.
- When appropriate, suggest one next analytic step (e.g., adjust threshold, compare categories, inspect selection).
- Never claim the tool replaces urban planners or that outputs are always globally optimal city-wide.
- If discussing optimality, scope it to the current candidate set, filters, and objective only.

View: {view}
Summary JSON:
{summary}
User question: {question}
"""



WHATIF_INTENT_TEMPLATE = """
You help a GIS app generate what-if suggestions for fairness improvements.
Return JSON ONLY with this schema:
  {{
    "categories": [string],
    "count": integer,
    "mode": "add" | "change" | null,
    "focus": "city" | "viewport" | "lasso" | null,
    "fairness_target": "overall" | "category" | null,
    "area": "any" | "center" | "outskirts" | null,
    "rationale": string
  }}

Rules:
- "categories" must be chosen from the allowed list in Context.
- For broad requests like "overall fairness", "increase fairness", or "what do I need to add" without a specific POI, return multiple categories (2-4) instead of a single default category.
- "count" should be between 1 and 5.
- "focus" should be "viewport" if the user wants city center/visible area, "lasso" if they mention a selected/lasso region, otherwise "city".
- If Context includes "focus_default", use it when the user is ambiguous.
- "fairness_target" should be "overall" if the user asks to improve overall fairness, otherwise "category".
- "area" should be "center" for city center, "outskirts" for rural/outside areas, otherwise "any".
- Output only JSON, no extra text.

Context:
{context}

User question:
{question}
"""

def _build_prompt(question: str, context: Optional[Dict[str, Any]]) -> str:
    ctx = json.dumps(context or {}, ensure_ascii=False, indent=2)
    return PROMPT_TEMPLATE.format(context=ctx, question=question.strip())


def _build_explain_prompt(view: str, summary: Dict[str, Any], question: Optional[str]) -> str:
    summary_json = json.dumps(summary or {}, ensure_ascii=False, indent=2)
    q = (question or "").strip() or "(no user question provided)"
    return EXPLAIN_TEMPLATE.format(view=view.strip(), summary=summary_json, question=q)


def _build_intent_prompt(question: str, context: Optional[Dict[str, Any]]) -> str:
    ctx = json.dumps(context or {}, ensure_ascii=False, indent=2)
    return WHATIF_INTENT_TEMPLATE.format(context=ctx, question=question.strip())

def _match_categories(question: str, context: Optional[Dict[str, Any]]) -> List[str]:
    ctx = context or {}
    allowed = ctx.get("categories") or []
    if not isinstance(allowed, list):
        allowed = []
    question_lc = question.lower()
    matches = []
    for cat in allowed:
        if not isinstance(cat, str):
            continue
        cat_lc = cat.lower()
        variants = {cat_lc}
        if cat_lc.endswith("y") and len(cat_lc) > 1:
            variants.add(f"{cat_lc[:-1]}ies")
        else:
            variants.add(f"{cat_lc}s")
        if any(re.search(rf"\b{re.escape(v)}\b", question_lc) for v in variants):
            matches.append(cat)
    if matches:
        return matches
    current = ctx.get("poi")
    if isinstance(current, str) and current:
        return [current]
    return []


def _extract_count(question: str) -> Optional[int]:
    match = re.search(r"\b(\d{1,3})\b", question)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
    
MOCK_TYPE_ALIASES = {
    "residential": [
        "residential",
        "residential building",
        "residential buildings",
        "housing",
        "housing unit",
        "housing units",
        "homes",
        "houses",
        "apartments",
    ],
    "grocery": ["grocery", "groceries", "supermarket", "market"],
    "hospital": ["hospital", "hospitals"],
    "pharmacy": ["pharmacy", "pharmacies", "drugstore", "chemist"],
    "dentistry": ["dentistry", "dentist", "dental clinic", "dental"],
    "healthcare_center": ["healthcare center", "health care center", "clinic", "medical center"],
    "veterinary": ["veterinary", "vet", "animal hospital"],
    "university": ["university", "college", "campus"],
    "kindergarten": ["kindergarten", "preschool", "nursery school"],
    "school_primary": ["primary school", "elementary school", "primary"],
    "school_high": ["high school", "secondary school", "secondary"],
}


def _extract_lasso_counts(question: str, context: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ctx = context or {}
    allowed = ctx.get("mock_types") or ctx.get("categories") or []
    allowed_set = {a for a in allowed if isinstance(a, str)}
    if not allowed_set:
        allowed_set = set(MOCK_TYPE_ALIASES.keys())
    question_lc = question.lower()
    counts: Dict[str, int] = {}

    def parse_number(raw: str) -> Optional[int]:
        try:
            return int(raw.replace(",", ""))
        except ValueError:
            return None

    for canonical, aliases in MOCK_TYPE_ALIASES.items():
        if canonical not in allowed_set:
            continue
        for alias in aliases:
            alias_pattern = re.escape(alias).replace("\\ ", "\\s+")
            pattern = re.compile(rf"\b(\d{{1,5}}(?:,\d{{3}})?)\s*(?:x\s*)?{alias_pattern}(?:s|es)?\b")
            for match in pattern.finditer(question_lc):
                raw = match.group(1)
                count = parse_number(raw)
                if count is None:
                    continue
                counts[canonical] = counts.get(canonical, 0) + count

    return [{"type": k, "count": v} for k, v in counts.items() if v > 0]


def _has_lasso_reference(question_lc: str) -> bool:
    return bool(
        re.search(
            r"\b(lasso|selected\s+area|selected\s+region|selected\s+zone|selected\s+polygon|selection|drawn\s+area)\b",
            question_lc,
        )
    )


def _extract_fairness_target(question_lc: str) -> Optional[str]:
    if re.search(r"\boverall\b", question_lc):
        return "overall"
    if re.search(r"\bcategory\b", question_lc):
        return "category"
    return None
    
def _match_weighted_categories(question: str, context: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ctx = context or {}
    allowed = ctx.get("categories") or []
    if not isinstance(allowed, list):
        return []
    question_lc = question.lower()
    results = []
    for cat in allowed:
        if not isinstance(cat, str):
            continue
        cat_lc = cat.lower()
        pattern = re.compile(
            rf"\b{re.escape(cat_lc)}\b[^0-9]{{0,25}}\bweight(?:s)?\b[^0-9]{{0,5}}(\d+(?:\.\d+)?)"
        )
        match = pattern.search(question_lc)
        if not match:
            continue
        try:
            weight = float(match.group(1))
        except ValueError:
            continue
        weight = max(1.0, min(10.0, weight))
        results.append({"cat": cat, "weight": weight})
    return results


def _detect_symbol_action(question_lc: str) -> Optional[Dict[str, Any]]:
    if not re.search(r"\b(symbols?|icons?)\b", question_lc):
        return None
    if re.search(r"\b(remove|hide|disable|turn off|no|without)\b", question_lc):
        return {"type": "SET_POI_SYMBOLS", "enabled": False}
    if re.search(r"\b(show|enable|turn on|with)\b", question_lc):
        return {"type": "SET_POI_SYMBOLS", "enabled": True}
    return None

def _extract_travel_mode(question_lc: str) -> Optional[str]:
    if re.search(r"\b(cycling|bike|bicycle)\b", question_lc):
        return "cycling"
    if re.search(r"\b(driving|car|vehicle)\b", question_lc):
        return "driving"
    if re.search(r"\b(walking|walk)\b", question_lc):
        return "walking"
    return None


def _detect_district_view_action(question_lc: str) -> Optional[Dict[str, Any]]:
    if not re.search(r"\bdistricts?\b", question_lc):
        return None
    if re.search(r"\b(hide|remove|disable|turn off)\b", question_lc):
        return {"type": "SET_DISTRICT_VIEW", "enabled": False}
    if re.search(r"\b(show|enable|turn on|view|mode)\b", question_lc):
        return {"type": "SET_DISTRICT_VIEW", "enabled": True}
    return None


def _extract_district_name(question: str) -> Optional[str]:
    match = re.search(
        r"\b(?:statistics?|stats?)\b(?:\s+of|\s+for)?\s+([^\n\r,.!?]+)",
        question,
        re.IGNORECASE,
    )
    if match:
        name = match.group(1).strip()
        return name or None
    match = re.search(
        r"\bdistricts?\b(?:\s+of|\s+for)?\s+([^\n\r,.!?]+)",
        question,
        re.IGNORECASE,
    )
    if match:
        name = match.group(1).strip()
        if not name:
            return None
        name_lc = name.lower()
        if name_lc in {"mode", "view", "border", "borders", "layer", "layers"}:
            return None
        return name
    return None


def _call_hf_inference(prompt: str) -> str:
    if not HF_TOKEN:
        return ""

    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    payload = {"inputs": prompt, "parameters": HF_PARAMS}

    try:
        r = requests.post(HF_API_URL, headers=headers, json=payload, timeout=(10, 60))
        r.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"LLM request failed: {e}") from e

    data = r.json()
    if isinstance(data, dict) and data.get("error"):
        raise HTTPException(status_code=502, detail=f"LLM error: {data['error']}")

    if isinstance(data, list) and data and isinstance(data[0], dict):
        text = data[0].get("generated_text") or ""
        return str(text)

    raise HTTPException(status_code=502, detail="Unexpected LLM response format")


def _call_ollama(prompt: str, *, force_json: bool = False) -> str:
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
    if force_json:
        payload["format"] = "json"
    try:
        r = requests.post(OLLAMA_API_URL, json=payload, timeout=(10, 120))
        r.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Ollama request failed: {e}") from e

    data = r.json()
    text = data.get("response") if isinstance(data, dict) else None
    if not isinstance(text, str):
        raise HTTPException(status_code=502, detail="Unexpected Ollama response format")
    return text


def _extract_plan(text: str) -> PlanResponse:
    if not text:
        return PlanResponse(actions=[{"type": "EXPLAIN", "message": "LLM not configured."}])

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise HTTPException(status_code=502, detail="LLM response contained no JSON")

    try:
        obj = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Failed to parse LLM JSON: {e}") from e

    actions = obj.get("actions") if isinstance(obj, dict) else None
    if not isinstance(actions, list):
        raise HTTPException(status_code=502, detail="LLM JSON missing 'actions' list")

    return PlanResponse(actions=_normalize_actions(actions))  # type: ignore[arg-type]


def _normalize_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    poi_actions = [a for a in actions if isinstance(a, dict) and a.get("type") == "SET_POI"]
    mix_action = next(
        (a for a in actions if isinstance(a, dict) and a.get("type") == "SET_POI_MIX"),
        None,
    )
    if not mix_action and len(poi_actions) > 1:
        categories = []
        for a in poi_actions:
            cat = a.get("category")
            if isinstance(cat, str):
                categories.append(cat)
        if categories:
            filtered = [a for a in actions if a not in poi_actions]
            filtered.append({"type": "SET_POI_MIX", "categories": categories})
            return filtered
    return actions

def _extract_json_object(text: str) -> Dict[str, Any]:
    if not text:
        raise HTTPException(status_code=502, detail="LLM response was empty")

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise HTTPException(status_code=502, detail="LLM response contained no JSON")

    try:
        obj = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Failed to parse LLM JSON: {e}") from e

    if not isinstance(obj, dict):
        raise HTTPException(status_code=502, detail="LLM JSON response is not an object")
    return obj


@router.post("/plan", response_model=PlanResponse)
def plan_actions(req: PlanRequest) -> PlanResponse:
    question = (req.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty")
    
    question_lc = question.lower()
    actions: List[Dict[str, Any]] = []
    symbol_action = _detect_symbol_action(question_lc)
    if symbol_action:
        actions.append(symbol_action)
    travel_mode = _extract_travel_mode(question_lc)
    if travel_mode:
        actions.append({"type": "SET_FAIRNESS_TRAVEL_MODE", "mode": travel_mode})

    district_view_action = _detect_district_view_action(question_lc)
    if district_view_action:
        actions.append(district_view_action)
    district_name = _extract_district_name(question)
    if district_name:
        actions.append({"type": "SHOW_DISTRICT_STATS", "district": district_name})
        if not district_view_action:
            actions.append({"type": "SET_DISTRICT_VIEW", "enabled": True})

    lasso_counts = _extract_lasso_counts(question, req.context)
    if _has_lasso_reference(question_lc) and re.search(r"\b(add|place|put|build|create|insert)\b", question_lc):
        if lasso_counts:
            actions.append({"type": "WHATIF_LASSO_APPLY", "counts": lasso_counts})
        else:
            actions.append({"type": "EXPLAIN", "message": "Provide counts for each building type to add."})
        if re.search(r"\b(what else|suggest|recommend|need to add|what should i add)\b", question_lc):
            ctx_categories = (req.context or {}).get("categories")
            if isinstance(ctx_categories, list):
                categories = [c for c in ctx_categories if isinstance(c, str)]
            else:
                categories = []
            payload: Dict[str, Any] = {"type": "WHATIF_SUGGEST", "prompt": question}
            if categories:
                payload["categories"] = categories
            payload["focus"] = "lasso"
            fairness_target = _extract_fairness_target(question_lc)
            if fairness_target:
                payload["fairness_target"] = fairness_target
            actions.append(payload)
        return PlanResponse(actions=actions)

    if _has_lasso_reference(question_lc) and re.search(r"\b(what else|suggest|recommend|need to add|what should i add)\b", question_lc):
        ctx_categories = (req.context or {}).get("categories")
        if isinstance(ctx_categories, list):
            categories = [c for c in ctx_categories if isinstance(c, str)]
        else:
            categories = []
        payload = {"type": "WHATIF_SUGGEST", "prompt": question, "focus": "lasso"}
        if categories:
            payload["categories"] = categories
        fairness_target = _extract_fairness_target(question_lc)
        if fairness_target:
            payload["fairness_target"] = fairness_target
        actions.append(payload)
        return PlanResponse(actions=actions)

    if re.search(r"\b(where|how many|how much|best)\b", question_lc) and re.search(
        r"\b(add|place|put)\b", question_lc
    ):
        categories = _match_categories(question_lc, req.context)
        count = _extract_count(question_lc)
        payload: Dict[str, Any] = {"type": "WHATIF_SUGGEST", "prompt": question}
        if categories:
            payload["categories"] = categories
        if count:
            payload["count"] = count
        actions.append(payload)
        return PlanResponse(actions=actions)

    if re.search(r"\bfair(ness)?\b", question_lc):
        weighted = _match_weighted_categories(question_lc, req.context)
        if weighted:
            actions.append(
                {
                    "type": "SET_POI_MIX",
                    "categories": [entry["cat"] for entry in weighted],
                    "weights": {entry["cat"]: entry["weight"] for entry in weighted},
                }
            )
            return PlanResponse(actions=actions)
        categories = _match_categories(question_lc, req.context)
        if not categories and re.search(r"\ball\b", question_lc):
            ctx_categories = (req.context or {}).get("categories")
            if isinstance(ctx_categories, list):
                categories = [c for c in ctx_categories if isinstance(c, str)]
        if categories:
            if len(categories) > 1:
                actions.append({"type": "SET_POI_MIX", "categories": categories})
            else:
                actions.append({"type": "SET_POI", "category": categories[0]})
            return PlanResponse(actions=actions)

    if actions:
        return PlanResponse(actions=actions)

    prompt = _build_prompt(question, req.context)
    text = _call_hf_inference(prompt)
    if not text:
        text = _call_ollama(prompt, force_json=True)
    try:
        return _extract_plan(text)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to build plan: {e}") from e


@router.post("/explain", response_model=ExplainResponse)
def explain_view(req: ExplainRequest) -> ExplainResponse:
    view = (req.view or "").strip()
    if not view:
        raise HTTPException(status_code=400, detail="View must not be empty")

    prompt = _build_explain_prompt(view, req.summary, req.question)
    text = _call_ollama(prompt)
    cleaned = text.strip()
    if not cleaned:
        raise HTTPException(status_code=502, detail="Empty response from LLM")
    return ExplainResponse(text=cleaned)


@router.post("/whatif-intent", response_model=WhatIfIntentResponse)
def whatif_intent(req: WhatIfIntentRequest) -> WhatIfIntentResponse:
    question = (req.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty")

    context = req.context or {}
    allowed = context.get("available_categories") or []
    if not isinstance(allowed, list):
        allowed = []

    prompt = _build_intent_prompt(question, context)
    text = _call_ollama(prompt, force_json=True)
    obj = _extract_json_object(text)

    categories = obj.get("categories") if isinstance(obj.get("categories"), list) else []
    categories = [c for c in categories if isinstance(c, str) and c in allowed]

    selected_categories = context.get("selected_categories") if isinstance(context.get("selected_categories"), list) else []
    selected_categories = [c for c in selected_categories if isinstance(c, str) and c in allowed]

    question_lc = question.lower()
    explicit_mentions = _match_categories(question, {"categories": allowed})
    broad_overall_request = bool(
        re.search(
            r"\b(overall|increase\s+fairness|improve\s+fairness|better\s+fairness|what\s+do\s+i\s+need\s+to\s+add|what\s+should\s+i\s+add)\b",
            question_lc,
        )
    ) and not explicit_mentions
    if not categories:
        if broad_overall_request and allowed:
            categories = selected_categories or allowed[: min(4, len(allowed))]
        elif selected_categories:
            categories = selected_categories
        elif context.get("current_category") in allowed:
            categories = [context.get("current_category")]
        elif allowed:
            categories = [allowed[0]]
        else:
            categories = []

    count = obj.get("count")
    if not isinstance(count, int):
        count = context.get("max_count") if isinstance(context.get("max_count"), int) else 1
    count = max(1, min(10, count))

    mode = obj.get("mode") if obj.get("mode") in {"add", "change"} else None
    focus = obj.get("focus") if obj.get("focus") in {"city", "viewport", "lasso"} else None
    city_scope_hint = re.search(r"\b(city[ -]?wide|entire city|inside the city|across the city|whole city)\b", question_lc)
    if city_scope_hint:
        focus = "city"
        
    fairness_target = (
        obj.get("fairness_target") if obj.get("fairness_target") in {"overall", "category"} else None
    )
    area = obj.get("area") if obj.get("area") in {"any", "center", "outskirts"} else None

    rationale = obj.get("rationale") if isinstance(obj.get("rationale"), str) else None

    return WhatIfIntentResponse(
        categories=categories,
        count=count,
        mode=mode,
        focus=focus,
        fairness_target=fairness_target,
        area=area,
        rationale=rationale,
    )