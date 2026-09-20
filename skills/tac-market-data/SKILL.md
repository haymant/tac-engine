---
name: tac-market-data
description: Guide agents to assemble the market-data inputs a fina-risk pricing request needs (correlation, implied-vol surface, interest-rate curve, FX and FX vol, dividends) from the tac-engine MCP tools, and to read them for any other purpose. Documents the required tac-engine MCP enhancements — calibrated vol surface (SVI/eSSVI), rate curve, and FX vol — with a 10-minute lake cache. Addon to tradeac-lake, tac-lake-macro and tradeac-alpaca (read those first).
---

# tac-market-data

One place to get the **market data that prices a structure**. The fina-risk
`pricing-request` carries a `market_data` block (`underlyings`, `curves`,
`vol_surfaces`, `correlations`); this skill maps each field to its tac-engine MCP
tool, states how fresh it must be, and specifies the MCP enhancements required
where no tool exists yet.

This is an **addon** to `tradeac-lake` (`get_lake_*`), `tac-lake-macro`
(`get_macro_*`, FRED rates) and `tradeac-alpaca` (`get_option_chain`,
`get_forex_*`, `get_corporate_actions`). Read those for the raw fetch tools; this
skill is the **assembly + calibration + caching** layer on top.

> **Build status:** correlation (`corr_matrix`) and the **vol surface
> (`vol_surface`)** are **shipped** (`corr.rs` / `vol.rs`). The rate curve
> (`rate_curve`) is built in FinAP from FRED tenors (`get_macro_bars`); the
> implied FX vol (`fx_vol_surface`) is pending (use realized `get_lake_fx_vol`).

## The `market_data` contract (what pricing needs)

The fina-risk skill consumes this shape (see `fina-risk/SKILL.md`):

```jsonc
"market_data": {
  "evaluation_date": 46272,                       // Excel serial
  "underlyings": [                                // spot + dividends per name
    {"id": "ADBE UW", "quoted_spot": 267.885, "reference_spot": 239.97,
     "currency": "USD", "dividends": [{"ex_date": 46332, "amount": 0.31}]}
  ],
  "curves": [                                     // discount + projection curve
    {"id": "USD", "day_count": "ACT/365",
     "pillars": [{"date": 46302, "rate": 0.037405}, ...]}
  ],
  "vol_surfaces": [                               // strike x maturity grid per name
    {"underlying": "ADBE UW", "strike_type": "absolute", "reference": 239.97,
     "maturities": [46299, 46330, ...], "strikes": [133.2, ...],
     "volatility": [[...], ...]}                  // percent or decimal per engine
  ],
  "correlations": [{"factors": ["ADBE UW", "AMZN UW"], "value": [0.4041]}]
}
```

**Rules:** ratio strikes/barriers are relative to `reference_spot` (the deal's
initial fixing), not the live `quoted_spot`; volatility is a **full grid**, not a
single ATM point; correlation is the **resolved** pairwise value, never 0.

## Source map — field → MCP tool

| `market_data` field | tac-engine MCP tool | skill | status |
|---------------------|---------------------|-------|--------|
| spot per underlying | `get_stock_snapshot` / `get_lake_bars` | tradeac-alpaca / tradeac-lake | shipped |
| dividends | `get_corporate_actions` (`type=dividend`) | tradeac-alpaca | shipped |
| `correlations` | **`corr_matrix`** (matrix), `corr_calc` (pair) | tradeac-lake | **shipped** |
| `vol_surfaces` | **`vol_surface`** (calibrated grid) | this skill | **spec** |
| `curves` (rates) | **`rate_curve`** (FRED pillars) | this skill / tac-lake-macro | **spec** |
| FX spot/forwards | `get_forex_latest_rates`, `get_forex_rates` | tradeac-alpaca | shipped |
| FX vol | **`get_lake_fx_vol`** (realized); implied = spec | tradeac-lake | partial |

## Correlation — shipped

`corr_matrix` (Kendall τ-b with dampening, `corr.rs`) is the primary source;
`corr_calc` returns one pair. Resolve the pair before pricing a worst-of — at
ρ=0 the short put is over-reserved. If `corr_matrix` is unavailable (e.g. no FX
config), fall back to lake daily closes (`get_lake_bars`) + Pearson.

```json
{"legs": "ADBE,AMZN", "window_days": 252, "alpha": 0.4}
```
→ `{"legs": ["ADBE","AMZN"], "matrix": [[1, 0.1694], [0.1694, 1]], "pairs": [{"leg_a":"ADBE","leg_b":"AMZN","rho0":0.1694,"rho":0.4067}]}`

Map `pairs[].rho` onto `market_data.correlations` using the **underlying ids**
(`factors: ["ADBE UW","AMZN UW"]`), not the tickers.

## Required enhancement 1 — `vol_surface` (calibrated surface, 10-min cache)

**Model.** Raw **SVI** per expiry (Gatheral total variance
`w(k)=a+b(ρ(k−m)+√((k−m)²+σ²))`, `k=log(K/F)`), fitted to OTM options and
vega/spread-weighted, with the no-butterfly constraints
`b(1+|ρ|)≤4`, `bσ√(1−ρ²)≤4`, `a+bσ√(1−ρ²)≥0`; wrapped across expiries as
**eSSVI** (single `ρ`, `η(θ)=ηγθ^{−γ}`, ATM `θ_t=σ_atm²t`) so the surface is
calendar-arbitrage-free. SVI-JW is the fallback reparameterization when slices
are sparse. Realized vol from `get_lake_bars` is the last-resort scalar.

**Tool contract.**

| tool | args | does |
|------|------|------|
| `vol_surface` | `{underlying, as_of?, max_age_secs=600, model="essvi", grid?: {strikes?, maturities?}}` | if the lake has a `(symbol, as_of)` surface calibrated within `max_age_secs`, **read it**; else fetch `get_option_chain`, calibrate, persist, return |
| `vol_surface_calibrate` | `{underlying, model?, window_days?}` | force recalibration + persist |
| `vol_surface_status` | `{underlying}` | `{calibrated_at, source, model, params, n_obs, stale}` |

**Output** — the fina-risk surface block, drop-in:

```json
{"underlying": "ADBE UW", "strike_type": "absolute", "reference": 239.97,
 "maturities": [46299, 46330, 46360], "strikes": [133.235, 159.882, 186.529],
 "volatility": [[48.77, 48.95, 49.16], [44.01, 39.55, 35.93], ...],
 "model": "essvi", "calibrated_at": "2026-09-20T13:05:00Z", "source": "lake"}
```

**Lake store** — `${TAC_LAKE_DIR}/vol_correlations… ` → the calibration cache is a
JSON sidecar per symbol: **`${TAC_LAKE_DIR}/vol_surfaces/<SYMBOL>.json`**
(`calibrated_at_epoch`, `grid_json` fields, `params`). A parquet table can replace
it later without changing the tool contract.

| col | type | notes |
|-----|------|-------|
| `t` | TIMESTAMPTZ | calibration time (freshness key) |
| `symbol` | VARCHAR | underlying id, e.g. `ADBE` |
| `model` | VARCHAR | `svi` \| `essvi` \| `svi_jw` \| `realized` |
| `reference` | DOUBLE | spot the grid is relative to |
| `grid_json` | VARCHAR | the surface block above |
| `params_json` | VARCHAR | SVI/eSSVI params (`a,b,ρ,m,σ` per slice; `ρ,γ,η`) |
| `n_obs` | INTEGER | option quotes used |
| `source` | VARCHAR | `alpaca` \| `lake` |

**Freshness rule (the 10-minute policy).** `vol_surface` reads the newest row for
`symbol` whose `t >= now() - max_age_secs` (default **600 s**); otherwise it
recalibrates and appends. `vol_surface_calibrate` bypasses the cache. This is the
same read-first / write-on-miss pattern as `get_lake_bars`.

## Required enhancement 2 — `rate_curve`

Wrap the FRED tenors (`tac-lake-macro`, `get_macro_bars`,
`FRED_DGS3MO…DGS30`) into a discount/projection curve matching the request:

| tool | args | does |
|------|------|------|
| `rate_curve` | `{currency="USD", as_of?, max_age_secs=600, interpolate="loglinear", day_count="ACT/365"}` | read the latest tenor rates (lake), bootstrap/parse into pillars, cache `rate_curves.parquet` |

Output → `market_data.curves[0]`:
```json
{"id": "USD", "day_count": "ACT/365",
 "pillars": [{"date": 46302, "rate": 0.0374}, {"date": 46637, "rate": 0.0414}]}
```
Do **not** collapse to a single pillar — the engine interpolates the curve to each
payment date. Use the tenors nearest the deal's expiry; keep `FRED_API_KEY` in the
env only (see `tac-lake-macro` secrets policy).

## Required enhancement 3 — `fx_vol_surface` (implied)

For non-USD payment currency, provide the FX spot/forward **and** vol:

| tool | args | does |
|------|------|------|
| `get_lake_fx_vol` (shipped) | `{pair, start?, end?, persist?}` | **realized** FX vol from `get_forex_rates` daily closes (`Lake::compute_fx_vol`) — use today |
| `fx_vol_surface` (spec) | `{pair, as_of?, max_age_secs=600}` | **implied** FX vol grid over tenors (ATM + risk-reversals/butterflies) when a source exists; same lake+freshness pattern |

Output is an equity-style grid keyed by pair, consumed as a quanto/composite leg
or as the FX factor alongside `correlations`. Until `fx_vol_surface` ships, use
`get_lake_fx_vol` (realized) for the FX vol input.

## Assembly recipe (agent checklist)

1. Resolve the underlying ids and the evaluation/expiry serials from the ticket.
2. `corr_matrix` → `market_data.correlations` (ids as factors).
3. `vol_surface` per underlying → `market_data.vol_surfaces` (full grid, reference
   = initial fixing).
4. `rate_curve` for the payment currency → `market_data.curves`.
5. `get_corporate_actions` → `underlyings[].dividends`; `get_stock_snapshot` →
   `quoted_spot`; the deal's `reference_spot` from the ticket.
6. Non-USD: `get_forex_*` + `fx_vol_surface`.
7. Validate: correlation non-zero for worst-of, ≥2 vol pillars per name, curve
   spans the payment dates. Then call `quote.price`.

## Engine-side consumption (implemented)

The pricing lane now **reads** the full curve/surface (previously flat):

| input | status |
|-------|--------|
| rates (drift) | `fcn_native.py` drift uses per-step `curve_rate_at(pillars, date)` |
| rates (discount) | `engine.cpp` discounts each cashflow at `curve_rate_at(pillars, settlement)` |
| vol | `fcn_native.py` reads the surface at **each fixing date** and both moneyness (skew + term structure) |

**Contract (both engines, keep in sync):**

1. **`curve_rate_at`** — linear in rate between bracketing pillars (ACT/365),
   flat beyond the ends. C++ `terms.hpp::curve_rate_at` and Python
   `fcn_native._curve_rate_at` are the single source of this rule. A one-pillar
   curve is flat (backward compatible).
2. **Surface read** — `_surface_point` stays linear in strike / linear in total
   variance across maturities, **and flat-extrapolates in vol** beyond the first/
   last pillar (dividing a flat total variance by a near-zero target otherwise
   blows up the front-dated vol). Drift uses `max(exercise 0.78, KI 0.70)` at each
   fixing date.
3. **Dual-curve** (`estCurves` drift vs `discCurves` discount) is still merged
   into one canonical `curves`; expose both when known.

A one-pillar curve / single-maturity grid degrades to the old flat behaviour, so
this is backward compatible — but the `rate_curve` pillars and `vol_surface` grid
are now the accuracy lever.

## References

- Raw fetch + lake cache/persistence: `tac-engine/skills/tradeac-lake/SKILL.md`.
- FRED rate/factor symbols: `tac-engine/skills/tac-lake-macro/SKILL.md`.
- Options / FX / corporate actions / snapshots: `tac-engine/skills/tradeac-alpaca/SKILL.md`.
- Consumer shape + the vol-read contract: `fina-risk/skills/fina-risk/SKILL.md`
  ("marketData", "Vol read").
