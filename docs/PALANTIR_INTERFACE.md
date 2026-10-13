# Patient Match Score — Palantir Model Interface

This document describes the integration between SurgeonAgent and the Palantir
Foundry live model deployment that produces per-surgeon **Patient Match Scores**.
Share with any developer who needs to call the model or maintain this integration.

Implementation lives in [`palantir_score.py`](../palantir_score.py).

---

## 1. Endpoint

```
POST https://informedsurgical.palantirfoundry.com/api/v2/models/liveDeployments/
     ri.foundry-ml-live.main.live-deployment.b6302c00-8e5e-4cb2-86f2-ec1e355becec/
     transformJson?preview=true
```

**Headers**

| Header | Value |
|---|---|
| `Content-Type` | `application/json` |
| `Authorization` | `Bearer <PALANTIR_BEARER_TOKEN>` |

The bearer token **rotates periodically**. SurgeonAgent reads it from the
environment on **every call** (never cached at startup), so rotating the value
in Railway takes effect immediately without a redeploy.

## 2. Request body

One surgeon + one patient profile per call:

```json
{
  "input": {
    "input_df": [
      {
        "InformedFeature": 0,
        "RoboticFeature": 0,
        "SurgeonRiskCategory": 1,
        "SurgeonRatingCategory": 1,
        "pos": "Outpatient Hospital",

        "HighBMIFeature": 1,
        "DiabetesType1Feature": 0,
        "DiabetesType2Feature": 1,
        "DiabetesAllFeature": 1,
        "PatientAgeCategory": 2,
        "PatientRiskCategory": 2,

        "npi": "1215917257",
        "inf_proc_group": "Knee Replacement",
        "sex": "M"
      }
    ]
  }
}
```

### Field reference

#### Constants (always these values, per model spec 2026-04-23)

| Field | Type | Value |
|---|---|---|
| `InformedFeature` | int | `0` |
| `RoboticFeature` | int | `0` |
| `SurgeonRiskCategory` | int | `1` |
| `SurgeonRatingCategory` | int | `1` |
| `pos` | string | `"Outpatient Hospital"` |

#### Patient-derived features

| Field | Type | Encoding |
|---|---|---|
| `HighBMIFeature` | int | `1` if BMI **> 30**, else `0` |
| `DiabetesType1Feature` | int | `1` if Type 1 diabetes, else `0` |
| `DiabetesType2Feature` | int | `1` if Type 2 diabetes, else `0` |
| `DiabetesAllFeature` | int | `1` if any diabetes (either type, or type unknown), else `0` |
| `PatientAgeCategory` | int | See age buckets below |
| `PatientRiskCategory` | int | See risk buckets below |
| `sex` | string | `"M"`, `"F"`, or `""` (declined to answer) |

**`PatientAgeCategory` buckets**

| Value | Age range |
|---|---|
| `1` | 70 and older |
| `2` | 55 – 69 |
| `3` | 40 – 44 |
| `4` | 20 – 39 |
| `0` | everything else (under 20, 45 – 54, or unknown) |

**`PatientRiskCategory` buckets** (from the "general health" intake question)

| Value | Meaning |
|---|---|
| `1` | Very Healthy |
| `2` | Healthy |
| `3` | One Chronic Condition |
| `4` | Multiple Chronic Conditions |
| `0` | unknown / no answer |

#### Per-surgeon identifiers

| Field | Type | Notes |
|---|---|---|
| `npi` | string | Surgeon's 10-digit NPI, from `NationalTop80Score.csv` |
| `inf_proc_group` | string | Full procedure name, exactly as it appears in the CSV `Procedure` column, e.g. `"Cholecystectomy"`, `"Knee Replacement"`, `"Hysterectomy - Benign"` |

## 3. Response

The score is the probability of **no complication event** — higher is better:

```json
{
  "output": {
    "output_df": [
      {
        "ComplicationEvent_proba_0": 0.9823,
        "ComplicationEvent_proba_1": 0.0177
      }
    ]
  }
}
```

| Field | Meaning |
|---|---|
| `ComplicationEvent_proba_0` | Probability of no complication (0.0 – 1.0). **This is the Patient Match Score.** |
| `ComplicationEvent_proba_1` | Probability of a complication (complement) |

**Display convention:** `round(ComplicationEvent_proba_0 * 100)` + `"% Match"`,
e.g. `0.9823` → **"98% Match"**.

The extractor in `palantir_score.py` (`extract_score()`) also tolerates several
fallback response shapes (`output.output_df[0].score`, `.prediction`, etc.) in
case the model output schema changes.

## 4. Operational behavior (SurgeonAgent client)

| Concern | Behavior | Env override |
|---|---|---|
| Read timeout | 30 s (model takes 1 – 10 s per call; longer on cold start) | `PALANTIR_TIMEOUT_SECONDS` |
| Connect timeout | 5 s | — |
| Retries | 3 attempts, exponential backoff (0.6 s / 1.2 s / 2.4 s); retried on 429 and 5xx | `PALANTIR_MAX_RETRIES` |
| Concurrency | Max 3 parallel calls when scoring a top-5 list (asyncio semaphore) | `PALANTIR_MAX_CONCURRENT` |
| Auth failure (401/403) | No retry; call returns no score | — |
| Any failure | Returns `None`; the agent omits the Patient Match column for that surgeon and continues | — |
| Debug | Set `PALANTIR_DEBUG=1` to log raw model responses | `PALANTIR_DEBUG` |
| Endpoint override | Hardcoded default; non-empty env var wins | `PALANTIR_MODEL_URL` |

**Required environment:** `PALANTIR_BEARER_TOKEN` (set in Railway → Variables).
If missing, scoring is skipped silently and the agent falls back to ranking by
Informed Score alone.

## 5. How SurgeonAgent uses the score

1. After `find_best_surgeon` returns the top 5 (plus any referred surgeon), the
   agent calls the model **once per surgeon** (parallelized, ≤3 concurrent).
2. The comparison table shown to the patient gets a **Patient Match** column
   (e.g. "98% Match").
3. **Recommendation tie-breaker:** if a surgeon's Informed Score is within
   5 points of the top-ranked surgeon AND their Patient Match Score is at least
   5 percentage points higher, the agent recommends that surgeon instead,
   with an explanation.
4. User-facing language describes it as "a personalized match score based on
   your health profile" — backend/vendor names are never mentioned to patients.

## 6. Quick test (Python)

```python
import requests, os

url = ("https://informedsurgical.palantirfoundry.com/api/v2/models/liveDeployments/"
       "ri.foundry-ml-live.main.live-deployment.b6302c00-8e5e-4cb2-86f2-ec1e355becec/"
       "transformJson?preview=true")

row = {
    "InformedFeature": 0, "RoboticFeature": 0,
    "SurgeonRiskCategory": 1, "SurgeonRatingCategory": 1,
    "pos": "Outpatient Hospital",
    "HighBMIFeature": 1, "DiabetesType1Feature": 0,
    "DiabetesType2Feature": 1, "DiabetesAllFeature": 1,
    "PatientAgeCategory": 2, "PatientRiskCategory": 2,
    "npi": "1215917257", "inf_proc_group": "Knee Replacement", "sex": "M",
}

resp = requests.post(
    url,
    json={"input": {"input_df": [row]}},
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.environ['PALANTIR_BEARER_TOKEN']}",
    },
    timeout=(5, 30),
)
resp.raise_for_status()
proba = resp.json()["output"]["output_df"][0]["ComplicationEvent_proba_0"]
print(f"{round(proba * 100)}% Match")
```

Or use the repo's built-in smoke test:

```bash
python palantir_score.py 1215917257 "Knee Replacement"
```
