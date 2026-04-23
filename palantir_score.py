"""
Palantir Foundry "Patient Match Score" integration.

Calls a live model deployment to get a per-surgeon match score for the patient.
Designed to fail gracefully — if the API is down or the token is missing, the
agent flow continues without match scores rather than blocking.

Environment variables:
  - PALANTIR_BEARER_TOKEN  required. Read on every call so Railway updates
                           apply without restarting.
  - PALANTIR_MODEL_URL     optional override for the transformJson endpoint.
"""

import os
import asyncio
import logging
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_MODEL_URL = (
    "https://informedsurgical.palantirfoundry.com/api/v2/models/liveDeployments/"
    "ri.foundry-ml-live.main.live-deployment.94b5c9b0-bdd5-48ab-93b7-5165daa182aa/"
    "transformJson?preview=true"
)

REQUEST_TIMEOUT_SECONDS = int(os.environ.get("PALANTIR_TIMEOUT_SECONDS", "30"))
MAX_RETRIES = int(os.environ.get("PALANTIR_MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = 0.6  # seconds; exponential: 0.6s, 1.2s, 2.4s
# Cap parallel requests — firing 5+ simultaneously can overwhelm a cold deployment
MAX_CONCURRENT_CALLS = int(os.environ.get("PALANTIR_MAX_CONCURRENT", "3"))

# Connect timeout can stay short; read timeout is the slow one for cold starts
_CONNECT_TIMEOUT = 5


# ---------------------------------------------------------------------------
# Patient-feature helpers — translate raw conversation answers into model inputs
# ---------------------------------------------------------------------------


def age_category(age: Optional[int]) -> int:
    """Return the PatientAgeCategory bucket.

      1 = 70+
      2 = 55 – 69
      3 = 40 – 44
      4 = 20 – 39
      0 = everything else (under 20, 45 – 54, or unknown)
    """
    if age is None:
        return 0
    if age >= 70:
        return 1
    if 55 <= age <= 69:
        return 2
    if 40 <= age <= 44:
        return 3
    if 20 <= age <= 39:
        return 4
    return 0


def risk_category(general_health: str) -> int:
    """Return the PatientRiskCategory bucket from the General Health answer.

      1 = Very Healthy
      2 = Healthy
      3 = One Chronic Condition
      4 = Multiple Chronic Conditions
      0 = unknown / no answer
    """
    if not general_health:
        return 0
    norm = general_health.strip().lower()
    if "very healthy" in norm:
        return 1
    if "multiple" in norm:  # check before plain "healthy" / "chronic"
        return 4
    if "one chronic" in norm or "single chronic" in norm:
        return 3
    if norm == "healthy":
        return 2
    return 0


def high_bmi_feature(bmi: Optional[float]) -> int:
    """1 if BMI > 30, else 0."""
    try:
        return 1 if (bmi is not None and float(bmi) > 30) else 0
    except (TypeError, ValueError):
        return 0


def sex_code(gender: str) -> str:
    """Return 'M', 'F', or '' (no answer / unknown)."""
    if not gender:
        return ""
    g = gender.strip().lower()
    if g.startswith("m") and "female" not in g:
        return "M"
    if g.startswith("f"):
        return "F"
    return ""


def diabetes_features(diabetes_status: str) -> dict:
    """Return the three diabetes feature flags from an answer string.

    Examples of accepted `diabetes_status`:
      "no" / "none"         -> all zeros
      "type 1"              -> Type1=1, All=1
      "type 2"              -> Type2=1, All=1
      "yes"/"diabetes"      -> All=1 (type unknown)
    """
    status = (diabetes_status or "").strip().lower()
    type1 = 1 if "type 1" in status or "type i" in status else 0
    type2 = 1 if "type 2" in status or "type ii" in status else 0
    has_any = 1 if (type1 or type2 or status in {"yes", "diabetes", "diabetic"}) else 0
    return {
        "DiabetesType1Feature": type1,
        "DiabetesType2Feature": type2,
        "DiabetesAllFeature": has_any,
    }


def build_patient_features(
    *,
    bmi: Optional[float],
    diabetes_status: str,
    age: Optional[int],
    general_health: str,
    gender: str,
) -> dict:
    """Package all patient-side features into a single dict.

    Does NOT include npi / inf_proc_group — those are per-surgeon and added
    by `get_match_score` at call time.
    """
    features = {
        "HighBMIFeature": high_bmi_feature(bmi),
        "PatientAgeCategory": age_category(age),
        "PatientRiskCategory": risk_category(general_health),
        "sex": sex_code(gender),
    }
    features.update(diabetes_features(diabetes_status))
    return features


# ---------------------------------------------------------------------------
# Score extraction — defensive, tries common Palantir response shapes
# ---------------------------------------------------------------------------


_SCORE_PATHS = [
    ("output", "output_df", 0, "score"),
    ("output", "output_df", 0, "prediction"),
    ("output_df", 0, "score"),
    ("output_df", 0, "prediction"),
    ("output", "score"),
    ("score",),
    ("prediction",),
]


def _walk(obj: Any, path: tuple) -> Any:
    try:
        cur = obj
        for key in path:
            cur = cur[key]
        return cur
    except (KeyError, IndexError, TypeError):
        return None


def extract_score(model_result: dict) -> Optional[float]:
    """Return a 0.0-1.0 score from the model response, or None if not found."""
    if not isinstance(model_result, dict):
        return None
    for path in _SCORE_PATHS:
        val = _walk(model_result, path)
        if val is None:
            continue
        try:
            f = float(val)
            # If it looks like 0-100, normalize
            if f > 1.0:
                f = f / 100.0
            return max(0.0, min(1.0, f))
        except (TypeError, ValueError):
            continue
    return None


def format_score_percent(score: Optional[float]) -> str:
    """Render a 0.0-1.0 score as a display string like '98% Match'."""
    if score is None:
        return ""
    return f"{round(score * 100)}% Match"


# ---------------------------------------------------------------------------
# HTTP call
# ---------------------------------------------------------------------------


def _call_palantir_sync(payload_row: dict) -> Optional[dict]:
    """Synchronous POST to the Foundry live deployment. Returns parsed JSON or None."""
    token = os.environ.get("PALANTIR_BEARER_TOKEN", "").strip()
    if not token:
        logger.warning("PALANTIR_BEARER_TOKEN is not set — skipping match score call")
        return None

    # Use env override if non-empty; otherwise fall back to the default URL.
    # (os.environ.get returns "" when a var is set but empty, not the default.)
    url = os.environ.get("PALANTIR_MODEL_URL", "").strip() or DEFAULT_MODEL_URL
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    body = {"input": {"input_df": [payload_row]}}

    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # (connect_timeout, read_timeout) — allow up to REQUEST_TIMEOUT_SECONDS
            # for the model to respond, since Palantir live deployments can take
            # 1-10s per call (longer on cold starts).
            resp = requests.post(
                url,
                json=body,
                headers=headers,
                timeout=(_CONNECT_TIMEOUT, REQUEST_TIMEOUT_SECONDS),
            )
            if resp.status_code == 401:
                logger.warning("Palantir 401 Unauthorized — token may have expired")
                return None
            if resp.status_code == 403:
                logger.warning("Palantir 403 Forbidden")
                return None
            if resp.ok:
                return resp.json()
            # 5xx / 429 → retry
            if resp.status_code >= 500 or resp.status_code == 429:
                last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                logger.warning(
                    f"Palantir retryable error (attempt {attempt}/{MAX_RETRIES}): {last_err}"
                )
            else:
                logger.warning(
                    f"Palantir non-retryable error HTTP {resp.status_code}: {resp.text[:200]}"
                )
                return None
        except requests.RequestException as e:
            last_err = e
            logger.warning(
                f"Palantir request exception (attempt {attempt}/{MAX_RETRIES}): {e}"
            )

        if attempt < MAX_RETRIES:
            import time
            time.sleep(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))

    logger.error(f"Palantir call failed after {MAX_RETRIES} attempts: {last_err}")
    return None


async def get_match_score(
    npi: str,
    inf_proc_group: str,
    patient_features: dict,
) -> Optional[float]:
    """Async wrapper: returns a 0.0-1.0 score, or None on any failure."""
    payload = {
        "HighBMIFeature": int(patient_features.get("HighBMIFeature", 0)),
        "DiabetesType1Feature": int(patient_features.get("DiabetesType1Feature", 0)),
        "DiabetesType2Feature": int(patient_features.get("DiabetesType2Feature", 0)),
        "DiabetesAllFeature": int(patient_features.get("DiabetesAllFeature", 0)),
        "PatientAgeCategory": int(patient_features.get("PatientAgeCategory", 0)),
        "PatientRiskCategory": int(patient_features.get("PatientRiskCategory", 0)),
        "npi": str(npi).strip(),
        "inf_proc_group": str(inf_proc_group).strip(),
        "sex": str(patient_features.get("sex", "")).strip(),
    }

    # Run the blocking requests call on a thread so we don't block the event loop
    result = await asyncio.to_thread(_call_palantir_sync, payload)
    if result is None:
        return None

    # First-call debug: log the raw structure so you can adjust _SCORE_PATHS if needed
    if os.environ.get("PALANTIR_DEBUG", "").strip() == "1":
        logger.info(f"Palantir raw response: {result}")

    score = extract_score(result)
    if score is None:
        logger.warning(
            f"Palantir response parsed but no score found. Keys: "
            f"{list(result.keys()) if isinstance(result, dict) else type(result).__name__}"
        )
    return score


async def score_surgeons(
    surgeons: list[dict],
    inf_proc_group: str,
    patient_features: dict,
) -> list[dict]:
    """Fetch match scores for multiple surgeons, throttled for endpoint safety.

    Each surgeon dict must have an `npi` key. Returns a list of dicts with
    added `match_score` (0.0-1.0 or None) and `match_score_display` ('98% Match' or '').

    Failures for individual surgeons are tolerated — they just get None scores.
    Concurrency is capped at MAX_CONCURRENT_CALLS to avoid overwhelming a
    cold deployment.
    """
    sem = asyncio.Semaphore(max(1, MAX_CONCURRENT_CALLS))

    async def _one(s: dict) -> dict:
        npi = s.get("npi", "")
        if not npi:
            return {**s, "match_score": None, "match_score_display": ""}
        async with sem:
            score = await get_match_score(npi, inf_proc_group, patient_features)
        return {
            **s,
            "match_score": score,
            "match_score_display": format_score_percent(score),
        }

    return await asyncio.gather(*(_one(s) for s in surgeons))


if __name__ == "__main__":
    # CLI smoke test:
    #   PALANTIR_BEARER_TOKEN=xxx python palantir_score.py 1234567890 Cholecystectomy
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 3:
        print("Usage: python palantir_score.py <npi> <procedure>")
        sys.exit(1)

    features = build_patient_features(
        bmi=32.5,
        diabetes_status="Type 2",
        age=62,
        general_health="Healthy",
        gender="Male",
    )
    print("Patient features:", features)

    score = asyncio.run(get_match_score(sys.argv[1], sys.argv[2], features))
    print("Score:", score, "->", format_score_percent(score))
