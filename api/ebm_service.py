# api/ebm_service.py
from typing import List, Dict, Any
from fastapi import APIRouter
from pydantic import BaseModel

import numpy as np
import pandas as pd
import traceback
import re

router = APIRouter(prefix="/api/ebm", tags=["ebm"])


class EBMRequest(BaseModel):
    X: List[List[float]]
    y: List[int]
    feature_names: List[str]


def _base_resp(note: str) -> Dict[str, Any]:
    """
    Helper to return a safe fallback response.
    """
    return {
        "mode": "ebm",
        "ranked": [],
        "note": note,
    }


def run_ebm_ranking(
    X: List[List[float]],
    y: List[int],
    feature_names: List[str],
) -> Dict[str, Any]:
    """
    Train an Explainable Boosting Machine (EBM) to separate
    selected vs non-selected buildings and return a ranked
    list of global feature importances.

    This version is VERY defensive: any internal error results
    in a safe response instead of a 500.
    """

    # --- Basic sanity checks, but never raise ---

    if not X or not y:
        return _base_resp("EBM: empty X or y – nothing to train on.")

    try:
        X_arr = np.asarray(X, dtype=float)
        y_arr = np.asarray(y, dtype=int)
    except Exception as e:
        traceback.print_exc()
        return _base_resp(f"EBM: failed to convert input to arrays: {type(e).__name__}: {e}")

    if X_arr.ndim != 2:
        return _base_resp("EBM: X must be a 2D array [n_samples, n_features].")

    n_samples, n_features = X_arr.shape

    if n_samples != len(y_arr):
        return _base_resp("EBM: X and y length mismatch in input.")

    if len(feature_names) != n_features:
        return _base_resp(
            f"EBM: feature_names length ({len(feature_names)}) "
            f"does not match X columns ({n_features})."
        )

    unique_labels = np.unique(y_arr)
    if len(unique_labels) < 2:
        # No contrast -> no meaningful model
        return _base_resp(
            "EBM: need at least one selected and one non-selected building "
            "to learn a contrast."
        )

    # --- Everything below is in one big try/except so nothing can crash the endpoint ---
    try:
        # Lazy import here so if interpret is not installed, we see a clean message
        from interpret.glassbox import ExplainableBoostingClassifier

        # Build DataFrame
        df = pd.DataFrame(X_arr, columns=feature_names)
        df["_y"] = y_arr

        # Optional downsampling to avoid crazy runtimes
        max_per_class = 2000
        dfs = []
        for label in unique_labels:
            df_label = df[df["_y"] == label]
            if len(df_label) > max_per_class:
                df_label = df_label.sample(max_per_class, random_state=42)
            dfs.append(df_label)

        df_small = pd.concat(dfs, axis=0)
        y_small = df_small["_y"].values
        df_small = df_small.drop(columns=["_y"])

        # Train EBM
        ebm = ExplainableBoostingClassifier(random_state=42, interactions=0)
        ebm.fit(df_small, y_small)

        # Global explanation via EBM term attributes (more reliable than explain_global)
        term_names = ebm.term_names_
        term_scores = ebm.term_importances()
        term_features = ebm.term_features_

        importances = []
        for tname, tscore, tfeats in zip(term_names, term_scores, term_features):
            if len(tfeats) != 1:   # skip interaction terms
                continue
            importances.append({"label": tname, "score": float(abs(tscore))})

        importances.sort(key=lambda d: d["score"], reverse=True)

        importances = [d for d in importances if not re.match(r'^feature\s+\d+$', d["label"], re.IGNORECASE)]

        if not importances:
            return _base_resp(
                "EBM: training succeeded but returned no feature importances."
            )

        return {
            "mode": "ebm",
            "ranked": importances,
            "note": (
                "Feature importance from Explainable Boosting Machine (EBM). "
                "Larger score = stronger relationship with being in the selection."
            ),
        }

    except Exception as e:
        # This catches *any* EBM / interpret / pandas / numpy error
        traceback.print_exc()
        return _base_resp(
            f"EBM crashed server-side: {type(e).__name__}: {e}"
        )


@router.post("/explain")
def explain_with_ebm(req: EBMRequest) -> Dict[str, Any]:
    """
    REST endpoint: POST /api/ebm/explain

    No response_model here so FastAPI won't try to re-validate our
    fallback error dict and turn it into another 500.
    """
    return run_ebm_ranking(
        X=req.X,
        y=req.y,
        feature_names=req.feature_names,
    )
