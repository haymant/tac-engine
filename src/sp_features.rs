//! Stochastic-process feature computation (`sp_*` columns) for the TradeAC lake.
//!
//! Rust port of `.tmp/exp/tac-qlib/examples/sp_features.py` on the
//! [`stochastic-rs`](https://github.com/rust-dd/stochastic-rs/) stack.
//! Family -> implementation mapping:
//!
//! | family      | implementation                                                                   |
//! |-------------|----------------------------------------------------------------------------------|
//! | `hmm`       | `stochastic_rs_stats::econometrics::hmm::GaussianHmm` (EM fit + posterior)        |
//! | `hurst`     | `stochastic_rs_stats::hurst::rs::RescaledRange` (rescaled-range R/S, Weron 2002)  |
//! | `jump`      | `stochastic_rs_stats::realized::{realized_variance, bipower_variation}` (windowed)|
//! | `moments`   | `stochastic_rs_stats::realized::{realized_skewness, realized_kurtosis, realized_semivariance}` (1/5/22d) |
//! | `ou`        | pure-Rust rolling AR(1) half-life + distance zscore                               |
//! | `har`       | pure-Rust rolling mean of squared returns (HAR-RV 1/5/22d) + RV autocorr / CV     |
//! | `trend`     | pure-Rust log-price momentum slopes + log level                                   |
//! | `signature` | pure-Rust path-signature lead/lag integrals (levels 1 & 2, lag 1 and 5)           |
//! | `momentum`  | pure-Rust multi-horizon total returns (22/63/126/252d) + 22d Sharpe-like ratio    |
//! | `garch`     | pure-Rust GARCH(1,1) MLE (variance-targeted, Nelder-Mead fit)                     |
//!
//! Column names carry the `sp_` prefix expected by the qlib `LakeFeatureProvider`
//! (consumed as `$sp_*` in handler `feature_fields`).
//!
//! # Deferred families (not implemented, out of scope for this pass)
//! - `entropy` — antropy's sample/permutation/spectral/SVD entropy; needs an FFT
//!   and an SVD backend.
//! - `catch22` — the 22-feature canonical time-series suite (pycatch22).

use std::collections::BTreeMap;

use ndarray::{Array1, Array2, ArrayView1};
use stochastic_rs_stats::econometrics::hmm::GaussianHmm;
use stochastic_rs_stats::hurst::rs::RescaledRange;
use stochastic_rs_stats::hurst::HurstEstimator;
use stochastic_rs_stats::realized::{
    bipower_variation, realized_kurtosis, realized_semivariance, realized_skewness,
    realized_variance,
};

/// All implemented SP feature families.
pub const FAMILIES: [&str; 10] = [
    "ou",
    "hmm",
    "jump",
    "har",
    "trend",
    "hurst",
    "signature",
    "moments",
    "momentum",
    "garch",
];

/// Rolling window used by each family (mirrors sp_features.py).
fn family_window(family: &str) -> usize {
    match family {
        "ou" => 60,
        "hmm" => 60,
        "jump" => 60,
        "har" => 22,
        "trend" => 60,
        "hurst" => 126,
        "signature" => 20,
        "moments" => 22,
        "momentum" => 252,
        "garch" => 252,
        _ => 60,
    }
}

/// Warm-up bars to pull before `start` so rolling windows render from the first
/// visible bar (and the HMM has a decent fit history). Capped like the TA path.
pub fn sp_lookback_bars(families: &[&str]) -> usize {
    let max_window = families
        .iter()
        .map(|f| family_window(f))
        .max()
        .unwrap_or(60);
    (max_window * 3).max(252).min(2000)
}

/// True when `name` is a known, implemented family.
pub fn known_sp_family(name: &str) -> bool {
    FAMILIES.iter().any(|f| *f == name)
}

// ---- small rolling helpers (numpy/pandas-equivalent) ----------------------

fn rolling_reduce(v: &[Option<f64>], window: usize, f: impl Fn(&[f64]) -> f64) -> Vec<Option<f64>> {
    let n = v.len();
    let mut out = vec![None; n];
    for i in window.saturating_sub(1)..n {
        let start = i + 1 - window;
        let mut ok = true;
        let mut buf = Vec::with_capacity(window);
        for x in &v[start..=i] {
            match x {
                Some(y) if y.is_finite() => buf.push(*y),
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            out[i] = Some(f(&buf));
        }
    }
    out
}

fn mean(x: &[f64]) -> f64 {
    x.iter().sum::<f64>() / x.len() as f64
}

fn sample_std(x: &[f64]) -> f64 {
    let m = mean(x);
    let n = x.len();
    if n < 2 {
        return 0.0;
    }
    let v = x.iter().map(|&v| (v - m).powi(2)).sum::<f64>() / (n - 1) as f64;
    v.sqrt()
}

fn rolling_mean(v: &[Option<f64>], w: usize) -> Vec<Option<f64>> {
    rolling_reduce(v, w, mean)
}

fn rolling_std(v: &[Option<f64>], w: usize) -> Vec<Option<f64>> {
    rolling_reduce(v, w, sample_std)
}

fn rolling_max(v: &[Option<f64>], w: usize) -> Vec<Option<f64>> {
    rolling_reduce(v, w, |x| {
        x.iter().copied().fold(f64::NEG_INFINITY, f64::max)
    })
}

/// Forward-fill None values (pandas `.ffill()`).
fn ffill(v: &[Option<f64>]) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(v.len());
    let mut last = None;
    for x in v {
        if x.is_some() {
            last = *x;
        }
        out.push(last);
    }
    out
}

/// OLS slope of `y ~ 1 + x` (numpy `polyfit(x, y, 1)[0]`).
fn ols_slope(x: &[f64], y: &[f64]) -> Option<f64> {
    let n = x.len();
    if n < 2 {
        return None;
    }
    let xm = mean(x);
    let ym = mean(y);
    let mut num = 0.0;
    let mut den = 0.0;
    for i in 0..n {
        let dx = x[i] - xm;
        num += dx * (y[i] - ym);
        den += dx * dx;
    }
    if den > 0.0 && den.is_finite() && num.is_finite() {
        Some(num / den)
    } else {
        None
    }
}

/// Sample lag-1 autocorrelation of `x` (Pearson correlation with the 1-step
/// shifted series, mean-centred over the whole window).
fn lag1_autocorr(x: &[f64]) -> Option<f64> {
    let n = x.len();
    if n < 3 {
        return None;
    }
    let m = mean(x);
    let mut num = 0.0;
    let mut den = 0.0;
    for &v in x {
        den += (v - m) * (v - m);
    }
    if den <= 0.0 || !den.is_finite() {
        return None;
    }
    for w in x.windows(2) {
        num += (w[0] - m) * (w[1] - m);
    }
    Some(num / den)
}

// ---- feature families -----------------------------------------------------

/// `ret` (always present, matches sp_features.build_features) + enabled families.
/// Each returned column is aligned 1:1 with `close`.
pub fn compute_sp_features(
    families: &[&str],
    close: &[Option<f64>],
    fit_end: Option<usize>,
) -> BTreeMap<String, Vec<Option<f64>>> {
    let n = close.len();
    let mut cols: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();

    // ret[i] = close[i]/close[i-1] - 1
    let mut ret = vec![None; n];
    for i in 1..n {
        if let (Some(c0), Some(c1)) = (close[i - 1], close[i]) {
            if c0 > 0.0 && c1 > 0.0 {
                ret[i] = Some(c1 / c0 - 1.0);
            }
        }
    }
    cols.insert("sp_ret".to_string(), ret.clone());

    let enabled = |f: &str| families.iter().any(|x| *x == f);

    if enabled("ou") {
        for (k, v) in ou_features(&ret, &close) {
            cols.insert(k, v);
        }
    }
    if enabled("hmm") {
        for (k, v) in hmm_features(&ret, fit_end) {
            cols.insert(k, v);
        }
    }
    if enabled("jump") {
        for (k, v) in jump_features(&ret, &close) {
            cols.insert(k, v);
        }
    }
    if enabled("har") {
        for (k, v) in har_features(&ret) {
            cols.insert(k, v);
        }
    }
    if enabled("trend") {
        for (k, v) in trend_features(&close) {
            cols.insert(k, v);
        }
    }
    if enabled("hurst") {
        cols.insert("sp_hurst_exponent".to_string(), hurst_features(&ret));
    }
    if enabled("moments") {
        for (k, v) in moments_features(&ret) {
            cols.insert(k, v);
        }
    }
    if enabled("signature") {
        for (k, v) in signature_features(&close) {
            cols.insert(k, v);
        }
    }
    if enabled("momentum") {
        for (k, v) in momentum_features(&close, &ret) {
            cols.insert(k, v);
        }
    }
    if enabled("garch") {
        for (k, v) in garch_features(&ret) {
            cols.insert(k, v);
        }
    }
    cols
}

/// OU mean reversion: rolling AR(1) half-life on log price + distance zscore.
fn ou_features(ret: &[Option<f64>], close: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let window = family_window("ou");
    let n = close.len();
    let logp: Vec<Option<f64>> = close
        .iter()
        .map(|c| c.filter(|v| *v > 0.0).map(|v| v.ln()))
        .collect();

    let rm = rolling_mean(close, window);
    let rv = rolling_std(close, window);
    let mut zscore = vec![None; n];
    for i in 0..n {
        if let (Some(m), Some(s)) = (rm[i], rv[i]) {
            if s > 0.0 {
                if let Some(c) = close[i] {
                    zscore[i] = Some((c - m) / s);
                }
            }
        }
    }

    // half-life from a rolling AR(1): beta = OLS slope of Δy_t on y_{t-1}.
    let mut half = vec![None; n];
    for i in window..n {
        // x = logp[i-window+1 .. i], xp = x[..-1], dy = diff(x)
        let x = &logp[i - window + 1..i];
        let xp: Vec<f64> = x[..x.len() - 1].iter().map(|v| v.unwrap()).collect();
        let dy: Vec<f64> = x
            .windows(2)
            .map(|w| w[1].unwrap() - w[0].unwrap())
            .collect();
        if xp.len() >= 2 {
            if let Some(beta) = ols_slope(&xp, &dy) {
                if beta < 0.0 {
                    half[i] = Some(-std::f64::consts::LN_2 / beta);
                }
            }
        }
    }

    let mut revert = vec![None; n];
    for (i, h) in half.iter().enumerate() {
        revert[i] = Some(if h.is_some() { 1.0 } else { -1.0 });
    }

    let mut out = BTreeMap::new();
    out.insert("sp_ou_zscore".to_string(), zscore);
    out.insert("sp_ou_half_life".to_string(), half);
    out.insert("sp_ou_revert".to_string(), revert);
    let _ = ret;
    out
}

/// 2-state Gaussian HMM on returns -> filtered regime probabilities.
///
/// Fit uses only returns with index `< fit_end` (no lookahead, like the Python
/// `fit_end=TRAIN_END`). The posterior is the *forward filter*
/// `P(s_t | y_1..y_t)` (`filtered_state_probs`), which is causal — the Python
/// reference used hmmlearn's smoothed `predict_proba`, so values are close but
/// not identical (verified on SPY: corr ~0.89, posterior MAE ~0.10). State
/// labels are canonicalized so state 0 = the higher-mean regime, matching the
/// reference's convention; verified ~91% state agreement on SPY.
fn hmm_features(ret: &[Option<f64>], fit_end: Option<usize>) -> BTreeMap<String, Vec<Option<f64>>> {
    let n = ret.len();
    let empty = || {
        let mut out = BTreeMap::new();
        out.insert("sp_hmm_p_regime1".to_string(), vec![None; n]);
        out.insert("sp_hmm_state".to_string(), vec![None; n]);
        out
    };

    let valid_idx: Vec<usize> = ret
        .iter()
        .enumerate()
        .filter_map(|(i, v)| v.filter(|x| x.is_finite()).map(|_| i))
        .collect();
    let fit_hi = fit_end.unwrap_or(n).min(n);
    let fit_idx: Vec<usize> = valid_idx.iter().copied().filter(|&i| i < fit_hi).collect();
    if valid_idx.len() < 50 || fit_idx.len() < 50 {
        return empty();
    }
    let fit: Vec<f64> = fit_idx.iter().map(|&i| ret[i].unwrap()).collect();

    // Deterministic init: split the fit returns by value (low/high regime).
    let mut sorted = fit.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = (sorted.len() / 2).max(1);
    let (lo, hi) = sorted.split_at(mid);
    let stat = |x: &[f64]| -> (f64, f64) {
        let m = mean(x);
        let v = if x.len() > 1 {
            x.iter().map(|&v| (v - m).powi(2)).sum::<f64>() / (x.len() - 1) as f64
        } else {
            1e-8
        };
        (m, v.max(1e-8).sqrt())
    };
    let (m0, s0) = stat(lo);
    let (m1, s1) = stat(hi);

    let mut hmm = GaussianHmm::new(
        Array1::from(vec![0.5, 0.5]),
        Array2::from_shape_vec((2, 2), vec![0.9, 0.1, 0.1, 0.9]).expect("2x2"),
        Array1::from(vec![m0, m1]),
        Array1::from(vec![s0.max(1e-8), s1.max(1e-8)]),
    );
    hmm.baum_welch(ArrayView1::from(&fit), 100, 1e-4);

    let obs: Array1<f64> = Array1::from(
        valid_idx
            .iter()
            .map(|&i| ret[i].unwrap())
            .collect::<Vec<_>>(),
    );
    let post = hmm.filtered_state_probs(obs.view());

    // Canonicalize the state ordering: HMM likelihood is invariant to a
    // permutation of the hidden states, so EM can converge to either labeling.
    // Pin state 0 = the higher-mean regime to match the reference's convention
    // (hmmlearn's state 0 in the .tmp/sp_features.py runs) so the output is
    // deterministic across reruns and comparable; otherwise repeated fits
    // could silently swap `p_regime1`/`state`.
    let means = hmm.means.clone();
    let flip = means[0] < means[1];

    let mut p_regime1 = vec![None; n];
    let mut state = vec![None; n];
    for (k, &i) in valid_idx.iter().enumerate() {
        let p0 = post[[k, 0]];
        let p1 = post[[k, 1]];
        // After the flip, state 0 = the (canonical) lower-mean regime.
        let (p0c, p1c) = if flip { (p1, p0) } else { (p0, p1) };
        p_regime1[i] = Some(p0c);
        let best_s = if p1c > p0c { 1 } else { 0 };
        state[i] = Some(best_s as f64);
    }

    let mut out = BTreeMap::new();
    out.insert("sp_hmm_p_regime1".to_string(), ffill(&p_regime1));
    out.insert("sp_hmm_state".to_string(), ffill(&state));
    out
}

/// Realized jump share via bipower variation + tail zscore jump flag.
fn jump_features(ret: &[Option<f64>], close: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let window = family_window("jump");
    let n = ret.len();
    let r: Vec<Option<f64>> = ret.iter().map(|x| x.map(|v| v.abs())).collect();

    // rv = rolling sum of ret^2 via stochastic-rs realized_variance over each window.
    let mut rv = vec![None; n];
    for i in (window - 1)..n {
        if ret[i + 1 - window..=i].iter().all(|x| x.is_some()) {
            let slice: Vec<f64> = ret[i + 1 - window..=i].iter().map(|x| x.unwrap()).collect();
            rv[i] = Some(realized_variance(ArrayView1::from(&slice)));
        }
    }
    // bv = π/2 * Σ |r_{j-1}||r_j| via stochastic-rs bipower_variation over a
    // window+1 slice (pandas shift(1)*r then rolling sum), NaN on the first
    // `window` rows like the shifted series.
    let mut bv = vec![None; n];
    for i in window..n {
        if r[i - window..=i].iter().all(|x| x.is_some()) {
            let slice: Vec<f64> = r[i - window..=i].iter().map(|x| x.unwrap()).collect();
            bv[i] = Some(bipower_variation(ArrayView1::from(&slice)));
        }
    }

    let mut jump_ratio = vec![None; n];
    for i in 0..n {
        jump_ratio[i] = Some(match (bv[i], rv[i]) {
            (Some(b), Some(v)) if b > 0.0 && v > 0.0 => (1.0 - b / v).clamp(0.0, 1.0),
            _ => 0.0,
        });
    }

    let rm = rolling_mean(&r, window);
    let rs = rolling_std(&r, window);
    let mut tail = vec![None; n];
    for i in 0..n {
        if let (Some(m), Some(s)) = (rm[i], rs[i]) {
            if s > 0.0 {
                if let Some(x) = r[i] {
                    tail[i] = Some((x - m) / s);
                }
            }
        }
    }
    let flag: Vec<Option<f64>> = tail
        .iter()
        .map(|t| match t {
            Some(z) => Some(if *z > 3.0 { 1.0 } else { 0.0 }),
            None => Some(0.0),
        })
        .collect();

    // max_move = rolling max of |pct_change| over the window
    let mut pct = vec![None; n];
    let mut up = vec![None; n];
    let mut down = vec![None; n];
    for i in 1..n {
        if let (Some(c0), Some(c1)) = (close[i - 1], close[i]) {
            if c0 > 0.0 {
                let s = (c1 - c0) / c0;
                pct[i] = Some(s.abs());
                up[i] = Some(s.max(0.0));
                down[i] = Some((-s).max(0.0));
            }
        }
    }
    let max_move = rolling_max(&pct, window);
    // Signed max-move asymmetry: rolling max of the up-side and down-side
    // magnitude of the daily move (extends sp_max_move with direction).
    let max_up = rolling_max(&up, window);
    let max_down = rolling_max(&down, window);

    let mut out = BTreeMap::new();
    out.insert("sp_jump_ratio".to_string(), jump_ratio);
    out.insert("sp_jump_flag".to_string(), flag);
    out.insert("sp_jump_tail".to_string(), tail);
    out.insert("sp_max_move".to_string(), max_move);
    out.insert("sp_max_up".to_string(), max_up);
    out.insert("sp_max_down".to_string(), max_down);
    out
}

/// HAR-style realized-volatility features at 1/5/22 day horizons.
fn har_features(ret: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let n = ret.len();
    let sq: Vec<Option<f64>> = ret.iter().map(|x| x.map(|v| v * v)).collect();
    let rv1 = sq.clone();
    let rv5 = rolling_mean(&sq, 5);
    let rv22 = rolling_mean(&sq, 22);
    let mut ratio_5_22 = vec![None; n];
    let mut ratio_1_22 = vec![None; n];
    for i in 0..n {
        if let Some(v22) = rv22[i] {
            if v22 > 0.0 {
                if let Some(v5) = rv5[i] {
                    ratio_5_22[i] = Some(v5 / v22);
                }
                if let Some(v1) = rv1[i] {
                    ratio_1_22[i] = Some(v1 / v22);
                }
            }
        }
    }
    let mut out = BTreeMap::new();
    out.insert("sp_rv1".to_string(), rv1);
    out.insert("sp_rv5".to_string(), rv5);
    out.insert("sp_rv22".to_string(), rv22);
    out.insert("sp_vol_ratio_5_22".to_string(), ratio_5_22);
    out.insert("sp_vol_ratio_1_22".to_string(), ratio_1_22);

    // Vol-of-vol / persistence beyond the level ratios:
    // sp_rv_ac1 = lag-1 autocorrelation of the 1d RV (squared return) series
    // over the family window; sp_rv_cv_22 = coefficient of variation of 1d RV.
    let ac_win = 22usize;
    let mut ac1 = vec![None; n];
    for i in (ac_win - 1)..n {
        if sq[i + 1 - ac_win..=i].iter().all(|x| x.is_some()) {
            let x: Vec<f64> = sq[i + 1 - ac_win..=i].iter().map(|x| x.unwrap()).collect();
            ac1[i] = lag1_autocorr(&x);
        }
    }
    let rv_mean = rolling_mean(&sq, 22);
    let rv_std = rolling_std(&sq, 22);
    let mut cv = vec![None; n];
    for i in 0..n {
        if let (Some(m), Some(s)) = (rv_mean[i], rv_std[i]) {
            if m > 0.0 {
                cv[i] = Some(s / m);
            }
        }
    }
    out.insert("sp_rv_ac1".to_string(), ac1);
    out.insert("sp_rv_cv_22".to_string(), cv);
    out
}

/// Realized higher moments (Amaya et al. 2015) via stochastic-rs `realized`,
/// windowed at 1/5/22 days: skewness (`RV^1.5`-normalized 3rd moment), kurtosis
/// (`RV^2`-normalized 4th moment) and downside semi-variance (squared returns
/// below zero) plus its share of total realized variance. All scale-free so they
/// survive the cross-sectional rank normalizer.
///
/// Realized skew/kurt are undefined for a 1-day window (needs ≥2 returns), so
/// only the 5/22-day variants are emitted for those two columns.
fn moments_features(ret: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let n = ret.len();
    let mut out = BTreeMap::new();
    for w in [1usize, 5, 22] {
        let mut dsv = vec![None; n];
        let mut dsv_ratio = vec![None; n];
        let mut rskew = vec![None; n];
        let mut rkurt = vec![None; n];
        for i in w.saturating_sub(1)..n {
            if ret[i + 1 - w..=i].iter().all(|x| x.is_some()) {
                let slice: Vec<f64> = ret[i + 1 - w..=i].iter().map(|x| x.unwrap()).collect();
                let view = ArrayView1::from(&slice);
                let rv = realized_variance(view);
                let (down, _up) = realized_semivariance(view, 0.0);
                dsv[i] = Some(down);
                dsv_ratio[i] = Some(if rv > 0.0 { down / rv } else { 0.0 });
                if w >= 2 {
                    rskew[i] = Some(realized_skewness(view));
                    rkurt[i] = Some(realized_kurtosis(view));
                }
            }
        }
        out.insert(format!("sp_dsv_{w}"), dsv);
        out.insert(format!("sp_dsv_ratio_{w}"), dsv_ratio);
        if w >= 2 {
            out.insert(format!("sp_rskew_{w}"), rskew);
            out.insert(format!("sp_rkurt_{w}"), rkurt);
        }
    }
    out
}

/// Rolling log-price momentum slopes over 5/20/60 days + log level.
fn trend_features(close: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let n = close.len();
    let logp: Vec<Option<f64>> = close
        .iter()
        .map(|c| c.filter(|v| *v > 0.0).map(|v| v.ln()))
        .collect();
    let mut out = BTreeMap::new();
    for w in [5usize, 20, 60] {
        let mut slope = vec![None; n];
        for i in w..n {
            if let (Some(l0), Some(l1)) = (logp[i - w], logp[i]) {
                slope[i] = Some((l1 - l0) / w as f64);
            }
        }
        out.insert(format!("sp_trend_slope_{w}"), slope);
    }
    out.insert("sp_logp".to_string(), logp);
    out
}

/// Rescaled-range (R/S) Hurst exponent over a rolling return window.
///
/// Uses `stochastic-rs` `RescaledRange` with `take_differences = false` (the
/// input is already a stationary returns series) and `anis_lloyd = true` (the
/// Anis–Lloyd (1976) small-sample correction: the expected R/S under iid normals
/// is subtracted at each scale, removing the finite-sample upward bias of the
/// raw R/S so an iid process estimates H ≈ 0.5). The Python reference's
/// `simplified=True` omits this correction, so values differ from the reference
/// but the estimate is statistically better grounded. On failure the last value
/// is carried forward (like the Python reference), then the column is ffill'd.
fn hurst_features(ret: &[Option<f64>]) -> Vec<Option<f64>> {
    let window = family_window("hurst");
    let n = ret.len();
    let mut out = vec![None; n];
    let mut last: Option<f64> = None;
    let rs = RescaledRange {
        anis_lloyd: true,
        take_differences: false,
        min_window: 10,
        max_window: None,
        n_windows: 12,
    };
    for i in window..n {
        let mut ok = true;
        let w: Vec<f64> = ret[i - window + 1..=i]
            .iter()
            .map(|x| match x {
                Some(v) if v.is_finite() => *v,
                _ => {
                    ok = false;
                    0.0
                }
            })
            .collect();
        if !ok {
            continue;
        }
        if sample_std(&w) == 0.0 {
            continue;
        }
        match rs.estimate(ArrayView1::from(&w)) {
            Ok(res) => {
                let h = res.hurst.clamp(0.0, 1.0);
                last = Some(h);
                out[i] = Some(h);
            }
            Err(_) => out[i] = last,
        }
    }
    ffill(&out)
}

/// Level-2 path-signature cross-integrals `∫X dY` / `∫Y dX` over two aligned
/// views of `ps` offset by `k`: the lead path drops the first `k` points, the
/// lag path drops the last `k`. Returns `(lead_lag, lag_lead)`.
fn sig2_cross(ps: &[Option<f64>], k: usize) -> Option<(f64, f64)> {
    if ps.len() <= k + 1 {
        return None;
    }
    let lead: Vec<f64> = ps[k..].iter().map(|x| x.unwrap()).collect();
    let lag: Vec<f64> = ps[..ps.len() - k].iter().map(|x| x.unwrap()).collect();
    let d_lead: Vec<f64> = lead.windows(2).map(|w| w[1] - w[0]).collect();
    let d_lag: Vec<f64> = lag.windows(2).map(|w| w[1] - w[0]).collect();
    let mut cs_lead = Vec::with_capacity(d_lead.len());
    let mut acc = 0.0;
    for &x in &d_lead {
        acc += x;
        cs_lead.push(acc);
    }
    let mut cs_lag = Vec::with_capacity(d_lag.len());
    let mut acc = 0.0;
    for &x in &d_lag {
        acc += x;
        cs_lag.push(acc);
    }
    let mut v2 = 0.0;
    let mut v3 = 0.0;
    for j in 0..cs_lead.len().saturating_sub(1) {
        v2 += cs_lead[j] * d_lag[j + 1];
        v3 += cs_lag[j] * d_lead[j + 1];
    }
    Some((v2, v3))
}

/// Path-signature lead/lag integrals (levels 1 & 2) over log-price paths.
///
/// Level-1 terms are the total log-return over the window (identical for any
/// lag shift), so only the level-2 cross-integrals are emitted for the longer
/// lag: `sp_sig_level2_{lead_lag,lag_lead}_5` use a 5-day offset instead of 1.
fn signature_features(close: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let window = family_window("signature");
    let n = close.len();
    // log_p = ln(close), forward- then backward-filled (pandas ffill().bfill()).
    let mut logp = vec![None; n];
    let mut last = None;
    for (i, c) in close.iter().enumerate() {
        if let Some(v) = c {
            if *v > 0.0 {
                last = Some(v.ln());
            }
        }
        logp[i] = last;
    }
    let mut bfill = vec![None; n];
    let mut next = None;
    for i in (0..n).rev() {
        if logp[i].is_some() {
            next = logp[i];
        }
        bfill[i] = next;
    }
    let logp = bfill;

    let mut s1_lead = vec![None; n];
    let mut s1_lag = vec![None; n];
    let mut s2_lead_lag = vec![None; n];
    let mut s2_lag_lead = vec![None; n];
    let mut s2_lead_lag_5 = vec![None; n];
    let mut s2_lag_lead_5 = vec![None; n];
    for i in window..n {
        let ps = &logp[i - window..=i];
        let lead: Vec<f64> = ps[1..].iter().map(|x| x.unwrap()).collect();
        let lag: Vec<f64> = ps[..ps.len() - 1].iter().map(|x| x.unwrap()).collect();
        let d_lead: Vec<f64> = lead.windows(2).map(|w| w[1] - w[0]).collect();
        let d_lag: Vec<f64> = lag.windows(2).map(|w| w[1] - w[0]).collect();
        s1_lead[i] = Some(d_lead.iter().sum());
        s1_lag[i] = Some(d_lag.iter().sum());

        for (k, ll, lgl) in [
            (1usize, &mut s2_lead_lag, &mut s2_lag_lead),
            (5, &mut s2_lead_lag_5, &mut s2_lag_lead_5),
        ] {
            if let Some((v2, v3)) = sig2_cross(ps, k) {
                ll[i] = Some(v2);
                lgl[i] = Some(v3);
            }
        }
    }

    let mut out = BTreeMap::new();
    out.insert("sp_sig_level1_lead".to_string(), s1_lead);
    out.insert("sp_sig_level1_lag".to_string(), s1_lag);
    out.insert("sp_sig_level2_lead_lag".to_string(), s2_lead_lag);
    out.insert("sp_sig_level2_lag_lead".to_string(), s2_lag_lead);
    out.insert("sp_sig_level2_lead_lag_5".to_string(), s2_lead_lag_5);
    out.insert("sp_sig_level2_lag_lead_5".to_string(), s2_lag_lead_5);
    out
}

/// Multi-horizon momentum: total (non-compounded) return over 22/63/126/252
/// days, plus a 22-day Sharpe-like ratio of mean daily return over the rolling
/// mean squared return (`sp_rv22`), computed self-contained in this family:
/// `sp_sharpe_22[i] = mean(ret[i-21..=i]) / sqrt(mean(ret[..]^2 over 22d))`.
fn momentum_features(
    close: &[Option<f64>],
    ret: &[Option<f64>],
) -> BTreeMap<String, Vec<Option<f64>>> {
    let n = close.len();
    let mut out = BTreeMap::new();
    for w in [22usize, 63, 126, 252] {
        let mut mret = vec![None; n];
        for i in w..n {
            if let (Some(c0), Some(c1)) = (close[i - w], close[i]) {
                if c0 > 0.0 && c1 > 0.0 {
                    mret[i] = Some(c1 / c0 - 1.0);
                }
            }
        }
        out.insert(format!("sp_ret_{w}"), mret);
    }
    let sq: Vec<Option<f64>> = ret.iter().map(|x| x.map(|v| v * v)).collect();
    let mean_ret = rolling_mean(ret, 22);
    let rv22 = rolling_mean(&sq, 22);
    let mut sharpe = vec![None; n];
    for i in 0..n {
        if let (Some(m), Some(v)) = (mean_ret[i], rv22[i]) {
            if v > 0.0 {
                sharpe[i] = Some(m / v.sqrt());
            }
        }
    }
    out.insert("sp_sharpe_22".to_string(), sharpe);
    out
}

/// GARCH(1,1) features over a rolling window of daily returns: the conditional
/// (one-step-ahead) variance forecast for each bar (`sp_garch_cond_var`), the
/// standardized residual of the window's last observation
/// (`sp_garch_std_resid`), and the persistence `alpha + beta`
/// (`sp_garch_persistence`).
///
/// Fits are causal (the window ends at the current bar, no lookahead) and
/// variance-targeted, so the recursion `sigma2_t = omega + alpha*r_{t-1}^2 +
/// beta*sigma2_{t-1}` with `omega = (1 - alpha - beta) * unconditional_variance`
/// is stationary whenever `alpha + beta < 1`. Failed / zero-variance fits carry
/// the last good value forward (ffill), mirroring `hurst_features`.
fn garch_features(ret: &[Option<f64>]) -> BTreeMap<String, Vec<Option<f64>>> {
    let window = family_window("garch");
    let n = ret.len();
    let mut cond_var = vec![None; n];
    let mut std_resid = vec![None; n];
    let mut persistence = vec![None; n];
    for i in window..n {
        let mut ok = true;
        let w: Vec<f64> = ret[i - window + 1..=i]
            .iter()
            .map(|x| match x {
                Some(v) if v.is_finite() => *v,
                _ => {
                    ok = false;
                    0.0
                }
            })
            .collect();
        if !ok {
            continue;
        }
        let m = mean(&w);
        let uv = w.iter().map(|r| (r - m).powi(2)).sum::<f64>() / w.len() as f64;
        if !(uv.is_finite() && uv > 0.0) {
            continue;
        }
        let Some((a, b)) = garch11_fit(&w, uv) else {
            continue;
        };
        let omega = (1.0 - a - b) * uv;
        let mut sigma2 = uv;
        let mut cur_var = uv;
        let mut cur_z = 0.0;
        for &r in &w {
            cur_var = sigma2;
            cur_z = r / sigma2.sqrt();
            sigma2 = omega + a * r * r + b * sigma2;
        }
        cond_var[i] = Some(cur_var);
        std_resid[i] = Some(cur_z);
        persistence[i] = Some(a + b);
    }
    let mut out = BTreeMap::new();
    out.insert("sp_garch_cond_var".to_string(), ffill(&cond_var));
    out.insert("sp_garch_std_resid".to_string(), ffill(&std_resid));
    out.insert("sp_garch_persistence".to_string(), ffill(&persistence));
    out
}

/// GARCH(1,1) MLE with variance targeting: the 2-D search is over `(alpha,
/// beta)` only, `omega` is pinned to `(1 - alpha - beta) * uv`, and feasibility
/// (`alpha, beta > 1e-4`, `alpha + beta < 0.999`) is enforced via an infinite
/// penalty so the optimizer never leaves the stationary region. Nelder-Mead is
/// a robust derivative-free drop-in for the 2-parameter case (the `arch`
/// package's `arch_model` uses bounded SLSQP/BFGS on the same likelihood).
fn garch11_fit(ret: &[f64], uv: f64) -> Option<(f64, f64)> {
    let (a, b) = nelder_mead(&|a, b| garch_neg_ll(a, b, ret, uv), (0.08, 0.85), 200, 1e-9)?;
    if a < 1e-4 || b < 1e-4 || a + b > 0.999 || !a.is_finite() || !b.is_finite() {
        return None;
    }
    Some((a, b))
}

/// Negative Gaussian log-likelihood of the GARCH(1,1) recursion over `ret`.
fn garch_neg_ll(alpha: f64, beta: f64, ret: &[f64], uv: f64) -> f64 {
    const MIN_A: f64 = 1e-4;
    const MIN_B: f64 = 1e-4;
    const MAX_P: f64 = 0.999;
    if !alpha.is_finite()
        || !beta.is_finite()
        || alpha < MIN_A
        || beta < MIN_B
        || alpha + beta > MAX_P
    {
        return f64::INFINITY;
    }
    let omega = (1.0 - alpha - beta) * uv;
    let mut sigma2 = uv;
    let mut ll = 0.0;
    for &r in ret {
        if !(sigma2.is_finite() && sigma2 > 0.0) {
            return f64::INFINITY;
        }
        ll += -0.5 * (sigma2.ln() + r * r / sigma2);
        sigma2 = omega + alpha * r * r + beta * sigma2;
    }
    -ll
}

/// Derivative-free Nelder-Mead minimization over 2 variables (3-vertex simplex),
/// returning the best vertex once the objective spread drops below `tol`.
fn nelder_mead(
    f: &impl Fn(f64, f64) -> f64,
    start: (f64, f64),
    max_iter: usize,
    tol: f64,
) -> Option<(f64, f64)> {
    const A_RFL: f64 = 1.0;
    const A_EXP: f64 = 2.0;
    const A_CON: f64 = 0.5;
    const A_SHR: f64 = 0.5;

    let mut vs = [
        start,
        (start.0 * 1.05 + 0.01, start.1),
        (start.0, start.1 * 1.05 + 0.01),
    ];
    let mut fs = [
        f(vs[0].0, vs[0].1),
        f(vs[1].0, vs[1].1),
        f(vs[2].0, vs[2].1),
    ];

    for _ in 0..max_iter {
        // order vertices by objective value (ascending)
        if fs[0] > fs[1] {
            vs.swap(0, 1);
            fs.swap(0, 1);
        }
        if fs[1] > fs[2] {
            vs.swap(1, 2);
            fs.swap(1, 2);
        }
        if fs[0] > fs[1] {
            vs.swap(0, 1);
            fs.swap(0, 1);
        }

        if fs[2] - fs[0] < tol {
            return Some(vs[0]);
        }
        if !(fs[0].is_finite() || fs[1].is_finite() || fs[2].is_finite()) {
            return None;
        }

        let c = ((vs[0].0 + vs[1].0) * 0.5, (vs[0].1 + vs[1].1) * 0.5);
        let xr = (
            c.0 + A_RFL * (c.0 - vs[2].0),
            c.1 + A_RFL * (c.1 - vs[2].1),
        );
        let fr = f(xr.0, xr.1);
        if fr < fs[0] {
            let xe = (
                c.0 + A_EXP * (xr.0 - c.0),
                c.1 + A_EXP * (xr.1 - c.1),
            );
            let fe = f(xe.0, xe.1);
            if fe < fr {
                vs[2] = xe;
                fs[2] = fe;
            } else {
                vs[2] = xr;
                fs[2] = fr;
            }
        } else if fr < fs[1] {
            vs[2] = xr;
            fs[2] = fr;
        } else {
            let xc = (
                c.0 + A_CON * (vs[2].0 - c.0),
                c.1 + A_CON * (vs[2].1 - c.1),
            );
            let fc = f(xc.0, xc.1);
            if fc < fs[2] {
                vs[2] = xc;
                fs[2] = fc;
            } else {
                vs[1] = (
                    vs[0].0 + A_SHR * (vs[1].0 - vs[0].0),
                    vs[0].1 + A_SHR * (vs[1].1 - vs[0].1),
                );
                fs[1] = f(vs[1].0, vs[1].1);
                vs[2] = (
                    vs[0].0 + A_SHR * (vs[2].0 - vs[0].0),
                    vs[0].1 + A_SHR * (vs[2].1 - vs[0].1),
                );
                fs[2] = f(vs[2].0, vs[2].1);
            }
        }
    }
    if fs[0] > fs[1] {
        vs.swap(0, 1);
        fs.swap(0, 1);
    }
    if fs[1] > fs[2] {
        vs.swap(1, 2);
        fs.swap(1, 2);
    }
    if fs[0] > fs[1] {
        vs.swap(0, 1);
        fs.swap(0, 1);
    }
    if fs[0].is_finite() {
        Some(vs[0])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(data: &[f64]) -> Vec<Option<f64>> {
        data.iter().map(|&x| Some(x)).collect()
    }

    #[test]
    fn rolling_helpers() {
        let v = series(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let m = rolling_mean(&v, 3);
        assert_eq!(m[2], Some(2.0));
        let sd = rolling_std(&v, 3);
        assert_eq!(sd[2], Some(1.0));
        let mx = rolling_max(&v, 3);
        assert_eq!(mx[2], Some(3.0));
    }

    #[test]
    fn ou_revert_defaults() {
        let close = series(&(0..30).map(|i| 100.0 + i as f64).collect::<Vec<_>>());
        let ret: Vec<Option<f64>> = vec![None; 30];
        let out = ou_features(&ret, &close);
        // before window=60 everything is None; but that needs 60+ points.
        assert_eq!(out["sp_ou_revert"][0], Some(-1.0));
    }

    #[test]
    fn trend_slope_exact() {
        let close = series(&(1..=10).map(|i| i as f64).collect::<Vec<_>>());
        let out = trend_features(&close);
        // logp diff(5)/5 at i=9: (ln10 - ln5)/5
        let expect = (10.0f64.ln() - 5.0f64.ln()) / 5.0;
        assert!((out["sp_trend_slope_5"][9].unwrap() - expect).abs() < 1e-12);
        assert_eq!(out["sp_trend_slope_5"][4], None);
        assert_eq!(out["sp_logp"][0].unwrap(), 0.0);
    }

    #[test]
    fn hurst_bounded() {
        // a pure random walk has H ~ 0.5; just check bounds + length
        let n = 200usize;
        let mut x = vec![0.0f64; n];
        let mut state: u64 = 42;
        for i in 1..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = (state >> 11) as f64 / (1u64 << 53) as f64;
            x[i] = x[i - 1] + (u - 0.5);
        }
        let ret = series(&x);
        let out = hurst_features(&ret);
        assert!(out[n - 1].unwrap().abs() <= 1.0);
        assert!(out[n - 1].unwrap() >= 0.0);
    }

    #[test]
    fn hurst_anis_lloyd_iid_centers_at_half() {
        // The Anis–Lloyd correction should make an iid normal process estimate
        // H ≈ 0.5 (unbiased), instead of the raw R/S's small-sample upward bias.
        let n = 600usize;
        let mut ret = vec![Some(0.0f64); n];
        let mut state: u64 = 99;
        for i in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = (state >> 11) as f64 / (1u64 << 53) as f64;
            // Box–Muller-ish approximation via sum of uniforms (zero-mean, iid)
            let v = (u - 0.5) * 2.0;
            ret[i] = Some(v);
        }
        let out = hurst_features(&ret);
        let last = out[n - 1].unwrap();
        assert!(
            (last - 0.5).abs() < 0.15,
            "Anis-Lloyd-corrected H should be near 0.5 for iid noise, got {last}"
        );
    }

    #[test]
    fn moments_windowed_matches_crate() {
        let ret = series(&[0.01, -0.02, 0.03, -0.01, 0.02, -0.03, 0.005, -0.005, 0.01, -0.01]);
        let out = moments_features(&ret);
        let slice: Vec<f64> = ret[5..=9].iter().map(|x| x.unwrap()).collect();
        let v = ArrayView1::from(&slice);
        assert!((out["sp_rskew_5"][9].unwrap() - realized_skewness(v)).abs() < 1e-12);
        assert!((out["sp_rkurt_5"][9].unwrap() - realized_kurtosis(v)).abs() < 1e-12);
        let (down, _up) = realized_semivariance(v, 0.0);
        assert!((out["sp_dsv_5"][9].unwrap() - down).abs() < 1e-12);
        let rv = realized_variance(v);
        assert!((out["sp_dsv_ratio_5"][9].unwrap() - down / rv).abs() < 1e-12);
        // downside + upside semivariance decompose into total RV over the window.
        assert!((down + realized_semivariance(v, 0.0).1 - rv).abs() < 1e-12);
        // window-1 skew/kurt are undefined (single return) and not emitted.
        assert!(!out.contains_key("sp_rskew_1"));
        assert!(!out.contains_key("sp_rkurt_1"));
        assert!(out.contains_key("sp_dsv_1"));
        assert!(out.contains_key("sp_dsv_22"));
    }

    #[test]
    fn jump_max_up_down_signed() {
        // window=60 needs a 70-bar series; constant prefix then a known tail.
        let mut close = vec![Some(100.0f64); 70];
        close[65] = Some(105.0);
        close[66] = Some(99.0);
        close[67] = Some(104.0);
        close[68] = Some(98.0);
        close[69] = Some(106.0);
        let ret: Vec<Option<f64>> = vec![None; 70];
        let out = jump_features(&ret, &close);
        // pct: +5%, -5.71%, +5.05%, -5.77%, +8.16%
        let max_up = out["sp_max_up"][69].unwrap();
        let max_down = out["sp_max_down"][69].unwrap();
        assert!((max_up - 106.0f64 / 98.0 + 1.0).abs() < 1e-9);
        assert!((max_down - (1.0 - 98.0 / 104.0)).abs() < 1e-9);
        assert!(max_up > max_down);
    }

    #[test]
    fn har_rv_ac_and_cv() {
        let ret: Vec<Option<f64>> = (1..=30).map(|i| Some((i as f64).sqrt())).collect();
        let out = har_features(&ret);
        // ac window = 22: index 29 covers sq = 9..=30 (squared returns).
        let sq: Vec<f64> = (9..=30).map(|i| i as f64).collect();
        assert!((out["sp_rv_ac1"][29].unwrap() - lag1_autocorr(&sq).unwrap()).abs() < 1e-12);
        let m = mean(&sq);
        let s = sample_std(&sq);
        assert!((out["sp_rv_cv_22"][29].unwrap() - s / m).abs() < 1e-12);
        assert_eq!(out["sp_rv_ac1"][20], None); // first full window starts at 21
    }

    #[test]
    fn signature_lag5_distinct() {
        let close = series(&(100..=150).map(|i| i as f64).collect::<Vec<_>>());
        let out = signature_features(&close);
        let i = close.len() - 1;
        assert!(out["sp_sig_level2_lead_lag_5"][i].is_some());
        assert!(out["sp_sig_level2_lag_lead_5"][i].is_some());
        // monotone log-price path: lag-5 cross terms differ from lag-1 ones.
        assert!((out["sp_sig_level2_lead_lag_5"][i].unwrap()
            - out["sp_sig_level2_lead_lag"][i].unwrap())
        .abs()
            > 1e-12);
        assert!((out["sp_sig_level2_lag_lead_5"][i].unwrap()
            - out["sp_sig_level2_lag_lead"][i].unwrap())
        .abs()
            > 1e-12);
    }

    #[test]
    fn lag1_autocorr_known_value() {
        // 1..=5 => num=4, den=10 => ac1 = 0.4.
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((lag1_autocorr(&x).unwrap() - 0.4).abs() < 1e-12);
        // constant series -> None (zero variance)
        assert_eq!(lag1_autocorr(&[2.0; 5]), None);
    }

    #[test]
    fn family_gating_emits_new_columns() {
        // random-ish positive price path long enough for every rolling window.
        let n = 260usize;
        let mut close = Vec::with_capacity(n);
        let mut x = 100.0f64;
        let mut state: u64 = 7;
        for _ in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = (state >> 11) as f64 / (1u64 << 53) as f64;
            x *= 1.0 + (u - 0.5) * 0.03;
            close.push(Some(x));
        }
        let out = compute_sp_features(&["jump", "har", "signature", "moments"], &close, None);
        for col in [
            "sp_max_up",
            "sp_max_down",
            "sp_rv_ac1",
            "sp_rv_cv_22",
            "sp_sig_level2_lead_lag_5",
            "sp_sig_level2_lag_lead_5",
            "sp_rskew_5",
            "sp_rskew_22",
            "sp_rkurt_5",
            "sp_rkurt_22",
            "sp_dsv_1",
            "sp_dsv_5",
            "sp_dsv_22",
            "sp_dsv_ratio_1",
            "sp_dsv_ratio_5",
            "sp_dsv_ratio_22",
        ] {
            assert!(out.contains_key(col), "missing {col}");
            assert_eq!(out[col].len(), n);
        }
        // a moments-only run must not leak jump/signature columns.
        let m = compute_sp_features(&["moments"], &close, None);
        assert!(m.contains_key("sp_rskew_22"));
        assert!(!m.contains_key("sp_max_up"));
        assert!(!m.contains_key("sp_rv_ac1"));
        // the full default set still emits the older families too.
        let all = compute_sp_features(&FAMILIES, &close, None);
        assert!(all.contains_key("sp_ou_zscore"));
        assert!(all.contains_key("sp_hurst_exponent"));
        assert!(all.contains_key("sp_sig_level1_lead"));
        assert!(all.contains_key("sp_ret_22"));
        assert!(all.contains_key("sp_ret_252"));
        assert!(all.contains_key("sp_sharpe_22"));
        assert!(all.contains_key("sp_garch_cond_var"));
        assert!(all.contains_key("sp_garch_std_resid"));
        assert!(all.contains_key("sp_garch_persistence"));
        // family gating: momentum-only run must not leak garch columns and vice-versa.
        let mm = compute_sp_features(&["momentum"], &close, None);
        assert!(mm.contains_key("sp_ret_63"));
        assert!(!mm.contains_key("sp_garch_persistence"));
        let gg = compute_sp_features(&["garch"], &close, None);
        assert!(gg.contains_key("sp_garch_persistence"));
        assert!(!gg.contains_key("sp_ret_22"));
    }

    #[test]
    fn momentum_returns_and_sharpe() {
        // linear price path c_i = 100 + i: sp_ret_w = c[i]/c[i-w] - 1.
        let n = 300usize;
        let close: Vec<Option<f64>> = (0..n).map(|i| Some(100.0 + i as f64)).collect();
        let ret: Vec<Option<f64>> = (0..n)
            .map(|i| {
                if i == 0 {
                    None
                } else {
                    Some(close[i].unwrap() / close[i - 1].unwrap() - 1.0)
                }
            })
            .collect();
        let out = momentum_features(&close, &ret);
        let i = n - 1;
        assert!(
            (out["sp_ret_22"][i].unwrap() - (100.0 + i as f64) / (100.0 + i as f64 - 22.0) + 1.0)
                .abs()
                < 1e-12
        );
        assert_eq!(out["sp_ret_22"][21], None);
        assert_eq!(out["sp_ret_252"][251], None);
        assert!(out["sp_ret_252"][i].unwrap() > 0.0);
        // sp_sharpe_22 = rolling mean(22) / sqrt(rolling mean of squares, 22).
        let rm: f64 = ret[i - 21..=i].iter().map(|x| x.unwrap()).sum::<f64>() / 22.0;
        let rv: f64 = ret[i - 21..=i]
            .iter()
            .map(|x| x.unwrap().powi(2))
            .sum::<f64>()
            / 22.0;
        assert!((out["sp_sharpe_22"][i].unwrap() - rm / rv.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn garch_bounded_on_synthetic() {
        // Data generated from a GARCH(1,1) recursion with a=0.10, b=0.85.
        let n = 320usize;
        let mut ret = vec![Some(0.0f64); n];
        let (a, b): (f64, f64) = (0.10, 0.85);
        let w = (1.0 - a - b) * 1.0;
        let mut sigma2: f64 = 1.0;
        let mut state: u64 = 1234;
        for i in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u1 = (state >> 11) as f64 / (1u64 << 53) as f64;
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u2 = (state >> 11) as f64 / (1u64 << 53) as f64;
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            let r = z * sigma2.sqrt();
            ret[i] = Some(r);
            sigma2 = w + a * r * r + b * sigma2;
        }
        let out = garch_features(&ret);
        let i = n - 1;
        let p = out["sp_garch_persistence"][i].unwrap();
        assert!(p > 0.0 && p < 0.999);
        assert!(out["sp_garch_cond_var"][i].unwrap() > 0.0);
        assert!((p - (a + b)).abs() < 0.2, "persistence {p} vs injected {}", a + b);
        assert!(out["sp_garch_std_resid"][i].is_some());
        // first full window starts at index window-1 -> nothing before it.
        assert_eq!(out["sp_garch_cond_var"][family_window("garch") - 1], None);
    }
}
