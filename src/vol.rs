//! Implied-volatility surface engine for the lake.
//!
//! Calibrates an equity implied-vol surface from an option chain: a raw **SVI**
//! slice (Gatheral total variance) per expiry, interpolated across expiries
//! **linearly in total variance** and returned on a strike x maturity grid that
//! the fina-risk pricing request consumes directly.
//!
//! Per-slice SVI (total variance in log-moneyness `k = ln(K/F)`):
//!
//! ```text
//! w(k) = a + b * (rho * (k - m) + sqrt((k - m)^2 + sigma^2))
//! ```
//!
//! Fitted to out-of-the-money total variances (`w = iv^2 * T`) by weighted
//! least squares with a Nelder-Mead simplex, under the no-butterfly constraints
//! (`b >= 0`, `|rho| < 1`, `sigma > 0`) enforced by parameter transforms. The
//! engine is pure math — callers supply the quotes; this module has no I/O.

use std::f64::consts::{PI, SQRT_2};

/// Standard normal CDF via the Abramowitz & Stegun 7.1.26 error-function
/// approximation (|eps| < 1.5e-7), which is ample for vol fitting.
pub fn norm_cdf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let z = x.abs() / SQRT_2;
    let t = 1.0 / (1.0 + 0.3275911 * z);
    let erf = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592)
            * t
            * (-z * z).exp();
    // erf(-z) = -erf(z); Phi(x) = 0.5 * (1 + erf(x/sqrt2))
    0.5 * (1.0 + sign * erf)
}

/// Standard normal PDF.
pub fn norm_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * PI).sqrt()
}

/// Black-Scholes-Merton price with continuous dividend yield `q`.
pub fn bs_price(spot: f64, strike: f64, rate: f64, div: f64, t: f64, vol: f64, is_call: bool) -> f64 {
    if t <= 0.0 || vol <= 0.0 || spot <= 0.0 || strike <= 0.0 {
        return intrinsic(spot, strike, t, rate, div, is_call);
    }
    let sqrt_t = t.sqrt();
    let d1 = ((spot / strike).ln() + (rate - div + 0.5 * vol * vol) * t) / (vol * sqrt_t);
    let d2 = d1 - vol * sqrt_t;
    let df_r = (-rate * t).exp();
    let df_q = (-div * t).exp();
    if is_call {
        spot * df_q * norm_cdf(d1) - strike * df_r * norm_cdf(d2)
    } else {
        strike * df_r * norm_cdf(-d2) - spot * df_q * norm_cdf(-d1)
    }
}

fn intrinsic(spot: f64, strike: f64, t: f64, rate: f64, div: f64, is_call: bool) -> f64 {
    let fwd = spot * (-div * t).exp();
    let df_r = (-rate * t).exp();
    if is_call {
        (fwd - strike * df_r).max(0.0)
    } else {
        (strike * df_r - fwd).max(0.0)
    }
}

/// Black-Scholes vega (per unit vol).
pub fn bs_vega(spot: f64, strike: f64, rate: f64, div: f64, t: f64, vol: f64) -> f64 {
    if t <= 0.0 || vol <= 0.0 {
        return 0.0;
    }
    let sqrt_t = t.sqrt();
    let d1 = ((spot / strike).ln() + (rate - div + 0.5 * vol * vol) * t) / (vol * sqrt_t);
    spot * (-div * t).exp() * norm_pdf(d1) * sqrt_t
}

/// Black-Scholes implied vol by bisection on `[1e-4, 5.0]`. Returns `None` when
/// the price sits outside the no-arbitrage band (stale/crossed quote).
pub fn implied_vol(price: f64, spot: f64, strike: f64, rate: f64, div: f64, t: f64, is_call: bool) -> Option<f64> {
    if t <= 0.0 || price <= 0.0 || spot <= 0.0 || strike <= 0.0 {
        return None;
    }
    let lower_arb = intrinsic(spot, strike, t, rate, div, is_call);
    let upper_arb = if is_call {
        spot * (-div * t).exp()
    } else {
        strike * (-rate * t).exp()
    };
    if price < lower_arb - 1e-9 || price > upper_arb + 1e-9 {
        return None;
    }
    let (mut lo, mut hi) = (1e-4_f64, 5.0_f64);
    let mut f_lo = bs_price(spot, strike, rate, div, t, lo, is_call) - price;
    let f_hi = bs_price(spot, strike, rate, div, t, hi, is_call) - price;
    if f_lo * f_hi > 0.0 {
        return None;
    }
    for _ in 0..200 {
        let mid = 0.5 * (lo + hi);
        let f_mid = bs_price(spot, strike, rate, div, t, mid, is_call) - price;
        if f_mid * f_lo <= 0.0 {
            hi = mid;
        } else {
            lo = mid;
            f_lo = f_mid;
        }
        if (hi - lo) < 1e-8 {
            break;
        }
    }
    let vol = 0.5 * (lo + hi);
    if vol.is_finite() && vol > 0.0 { Some(vol) } else { None }
}

/// Raw SVI slice (total variance in log-moneyness `k = ln(K/F)`).
#[derive(Clone, Copy, Debug)]
pub struct Svi {
    pub a: f64,
    pub b: f64,
    pub rho: f64,
    pub m: f64,
    pub sigma: f64,
}

impl Svi {
    /// Total implied variance `w(k) = sigma_bs(k)^2 * T`.
    pub fn total_variance(&self, k: f64) -> f64 {
        let d = k - self.m;
        let s = (d * d + self.sigma * self.sigma).sqrt();
        (self.a + self.b * (self.rho * d + s)).max(0.0)
    }

    /// Implied vol for maturity `t`.
    pub fn vol(&self, k: f64, t: f64) -> f64 {
        if t <= 0.0 {
            return 0.0;
        }
        (self.total_variance(k) / t).max(0.0).sqrt()
    }
}

/// One `(k, total_variance, weight)` observation for a slice.
pub type Point = (f64, f64, f64);

fn svi_from(q: &[f64]) -> Svi {
    Svi { a: q[0], b: q[1], rho: q[2], m: q[3], sigma: q[4] }
}

/// Weighted SSE with a hard penalty that keeps the slice inside the
/// no-butterfly region (`b > 0`, `sigma > 0`, `|rho| < 1`).
fn penalized_sse(q: &[f64], points: &[Point]) -> f64 {
    if q[1] < 1e-6 || q[4] < 1e-6 || q[2].abs() >= 0.999 {
        return 1e9 + sse(&svi_from(q), points);
    }
    sse(&svi_from(q), points)
}

fn sse(svi: &Svi, points: &[Point]) -> f64 {
    points
        .iter()
        .map(|&(k, w, weight)| {
            let r = svi.total_variance(k) - w;
            weight * r * r
        })
        .sum()
}

/// Nelder-Mead simplex minimisation of `f` over `n` dimensions.
fn nelder_mead<F: Fn(&[f64]) -> f64>(f: F, mut simplex: Vec<Vec<f64>>, steps: usize) -> Vec<f64> {
    let n = simplex.len() - 1;
    let mut vals: Vec<f64> = simplex.iter().map(|p| f(p)).collect();
    for _ in 0..steps {
        let mut order: Vec<usize> = (0..=n).collect();
        order.sort_by(|&i, &j| vals[i].partial_cmp(&vals[j]).unwrap_or(std::cmp::Ordering::Equal));
        let best = simplex[order[0]].clone();
        let worst = order[n];
        let second_worst = order[n - 1];
        let mut centroid = vec![0.0; n];
        for &i in order.iter().take(n) {
            for d in 0..n {
                centroid[d] += simplex[i][d] / n as f64;
            }
        }
        // Reflection
        let reflected: Vec<f64> = (0..n).map(|d| centroid[d] + (centroid[d] - simplex[worst][d])).collect();
        let fr = f(&reflected);
        if fr < vals[order[0]] {
            // Expansion
            let expanded: Vec<f64> = (0..n).map(|d| centroid[d] + 2.0 * (reflected[d] - centroid[d])).collect();
            let fe = f(&expanded);
            if fe < fr {
                simplex[worst] = expanded;
                vals[worst] = fe;
            } else {
                simplex[worst] = reflected;
                vals[worst] = fr;
            }
        } else if fr < vals[second_worst] {
            simplex[worst] = reflected;
            vals[worst] = fr;
        } else {
            // Contraction
            let contracted: Vec<f64> = (0..n)
                .map(|d| centroid[d] + 0.5 * (simplex[worst][d] - centroid[d]))
                .collect();
            let fc = f(&contracted);
            if fc < vals[worst] {
                simplex[worst] = contracted;
                vals[worst] = fc;
            } else {
                // Shrink toward best
                for &i in order.iter().skip(1) {
                    for d in 0..n {
                        simplex[i][d] = best[d] + 0.5 * (simplex[i][d] - best[d]);
                    }
                    vals[i] = f(&simplex[i]);
                }
            }
        }
        let spread = vals
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max)
            - vals.iter().cloned().fold(f64::INFINITY, f64::min);
        if spread < 1e-12 {
            break;
        }
    }
    let mut order: Vec<usize> = (0..=n).collect();
    order.sort_by(|&i, &j| vals[i].partial_cmp(&vals[j]).unwrap_or(std::cmp::Ordering::Equal));
    simplex[order[0]].clone()
}

/// Fit a raw SVI slice to `(k, total_variance, weight)` points (e.g. OTM quotes
/// with vega/spread weights). `t` is only used for the initial ATM guess.
pub fn fit_svi(points: &[Point], t: f64) -> Svi {
    if points.is_empty() || t <= 0.0 {
        return Svi { a: 0.0, b: 0.1, rho: 0.0, m: 0.0, sigma: 0.1 };
    }
    let w0 = points
        .iter()
        .min_by(|a, b| a.0.abs().partial_cmp(&b.0.abs()).unwrap_or(std::cmp::Ordering::Equal))
        .map(|p| p.1)
        .unwrap_or(0.04)
        .max(1e-6);
    let seed = vec![w0 * 0.5, 0.1, -0.2, 0.0, 0.1];
    let steps = [w0 * 0.5 + 1e-4, 0.05, 0.2, 0.05, 0.05];
    let mut simplex = vec![seed.clone()];
    for d in 0..5 {
        let mut p = seed.clone();
        p[d] += steps[d];
        simplex.push(p);
    }
    let q = nelder_mead(|p| penalized_sse(p, points), simplex, 8000);
    svi_from(&q)
}

/// One calibrated expiry.
#[derive(Clone, Debug)]
pub struct VolSlice {
    pub t: f64,
    pub forward: f64,
    pub svi: Svi,
}

/// A calibrated surface: SVI slices sorted by maturity.
#[derive(Clone, Debug)]
pub struct VolSurface {
    pub spot: f64,
    pub reference: f64,
    pub slices: Vec<VolSlice>,
}

impl VolSurface {
    /// Interpolate total variance at a maturity, linearly in `t`, at the
    /// slice-relative log-moneyness `k` (SVI evaluated per slice).
    pub fn total_variance_at(&self, t: f64, k: f64) -> f64 {
        if self.slices.is_empty() {
            return 0.04 * t.max(0.0);
        }
        if t <= self.slices[0].t {
            return self.slices[0].svi.total_variance(k);
        }
        let last = &self.slices[self.slices.len() - 1];
        if t >= last.t {
            return last.svi.total_variance(k);
        }
        for w in self.slices.windows(2) {
            let (lo, hi) = (&w[0], &w[1]);
            if t >= lo.t && t <= hi.t {
                let wl = lo.svi.total_variance(k);
                let wh = hi.svi.total_variance(k);
                let f = (t - lo.t) / (hi.t - lo.t);
                return wl + f * (wh - wl);
            }
        }
        last.svi.total_variance(k)
    }

    /// Implied vol at `(absolute strike, maturity)` given the spot.
    pub fn vol_at(&self, strike: f64, t: f64) -> f64 {
        if t <= 0.0 || strike <= 0.0 || self.spot <= 0.0 {
            return 0.0;
        }
        let k = (strike / self.spot).ln();
        (self.total_variance_at(t, k) / t).max(0.0).sqrt()
    }

    /// Surface on a strike x maturity grid: `grid[m][s]` = vol at `maturities[m]`,
    /// `strikes[s]`.
    pub fn vol_grid(&self, strikes: &[f64], maturities: &[f64]) -> Vec<Vec<f64>> {
        maturities
            .iter()
            .map(|&t| strikes.iter().map(|&k| self.vol_at(k, t)).collect())
            .collect()
    }

    /// Butterfly (per slice) + calendar (total variance non-decreasing in `t` at
    /// fixed `k`) arbitrage screen. Returns human-readable violations.
    pub fn arbitrage_violations(&self) -> Vec<String> {
        let mut out = Vec::new();
        for slice in &self.slices {
            let s = slice.svi;
            if s.b < 0.0 {
                out.push(format!("t={:.3}: negative SVI b", slice.t));
            }
            if s.rho.abs() >= 1.0 {
                out.push(format!("t={:.3}: |rho| >= 1", slice.t));
            }
            if s.sigma <= 0.0 {
                out.push(format!("t={:.3}: non-positive SVI sigma", slice.t));
            }
            // Roger Lee wing bound / no-butterfly sufficiency.
            if s.b * (1.0 + s.rho.abs()) > 4.0 {
                out.push(format!("t={:.3}: butterfly bound b(1+|rho|) > 4", slice.t));
            }
        }
        let ks = [-1.5, -0.75, -0.25, 0.0, 0.25, 0.75, 1.5];
        for w in self.slices.windows(2) {
            for &k in &ks {
                let wl = w[0].svi.total_variance(k);
                let wh = w[1].svi.total_variance(k);
                if wh + 1e-9 < wl {
                    out.push(format!("calendar violation k={k}: w({:.2}) < w({:.2})", w[1].t, w[0].t));
                    break;
                }
            }
        }
        out
    }
}

/// Build and calibrate a surface from per-expiry `(t, forward, points)` where
/// each point is `(log_moneyness, total_variance, weight)`.
pub fn calibrate(spot: f64, reference: f64, slices: Vec<(f64, f64, Vec<Point>)>) -> VolSurface {
    let mut out: Vec<VolSlice> = slices
        .into_iter()
        .filter(|(t, _, pts)| *t > 0.0 && !pts.is_empty())
        .map(|(t, forward, pts)| VolSlice { t, forward, svi: fit_svi(&pts, t) })
        .collect();
    out.sort_by(|a, b| a.t.partial_cmp(&b.t).unwrap_or(std::cmp::Ordering::Equal));
    VolSurface { spot, reference, slices: out }
}

/// One market option quote feeding calibration (`iv` decimal, `t` in years).
#[derive(Clone, Copy, Debug)]
pub struct Quote {
    pub t: f64,
    pub strike: f64,
    pub iv: f64,
    pub is_call: bool,
    pub weight: f64,
}

/// Group quotes by expiry into OTM `(k, w, weight)` points (`k = ln(K/spot)`,
/// `w = iv^2 * t`). Calls are used for `k > 0`, puts for `k <= 0`, so the fit
/// sees the liquid OTM wings. Expiries with fewer than 3 points are dropped.
pub fn points_by_expiry(spot: f64, quotes: &[Quote]) -> Vec<(f64, Vec<Point>)> {
    use std::collections::BTreeMap;
    let mut by_t: BTreeMap<i64, Vec<Point>> = BTreeMap::new();
    for q in quotes {
        if q.t <= 0.0 || q.iv <= 0.0 || q.strike <= 0.0 || spot <= 0.0 {
            continue;
        }
        let k = (q.strike / spot).ln();
        if (k > 0.0) != q.is_call {
            continue; // keep OTM only
        }
        let w = q.iv * q.iv * q.t;
        by_t.entry((q.t * 1e6).round() as i64).or_default().push((k, w, q.weight.max(1e-6)));
    }
    by_t.into_iter()
        .filter_map(|(key, mut pts)| {
            if pts.len() < 3 {
                return None;
            }
            pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            Some((key as f64 / 1e6, pts))
        })
        .collect()
}

/// Calibrate a surface directly from option quotes (forward ≈ spot).
pub fn calibrate_quotes(spot: f64, reference: f64, quotes: &[Quote]) -> VolSurface {
    let slices = points_by_expiry(spot, quotes)
        .into_iter()
        .map(|(t, pts)| (t, spot, pts))
        .collect();
    calibrate(spot, reference, slices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bs_call_put_parity() {
        let (s, k, r, q, t, v) = (100.0, 100.0, 0.03, 0.01, 1.0, 0.2);
        let c = bs_price(s, k, r, q, t, v, true);
        let p = bs_price(s, k, r, q, t, v, false);
        let lhs = c - p;
        let rhs = s * (-q * t).exp() - k * (-r * t).exp();
        assert!((lhs - rhs).abs() < 1e-8, "parity {lhs} vs {rhs}");
    }

    #[test]
    fn implied_vol_round_trips() {
        for &vol in &[0.1, 0.25, 0.6] {
            let price = bs_price(100.0, 105.0, 0.02, 0.0, 0.5, vol, true);
            let iv = implied_vol(price, 100.0, 105.0, 0.02, 0.0, 0.5, true).unwrap();
            assert!((iv - vol).abs() < 1e-5, "iv {iv} vs {vol}");
        }
    }

    #[test]
    fn svi_recovers_synthetic_slice() {
        let truth = Svi { a: 0.02, b: 0.15, rho: -0.3, m: 0.0, sigma: 0.08 };
        let t = 1.0;
        let mut pts = Vec::new();
        for i in 0..21 {
            let k = -0.6 + 0.06 * i as f64;
            pts.push((k, truth.total_variance(k), 1.0));
        }
        let fit = fit_svi(&pts, t);
        let err: f64 = pts
            .iter()
            .map(|&(k, w, _)| (fit.total_variance(k) - w).abs())
            .fold(0.0, f64::max);
        assert!(err < 5e-4, "max total-variance error {err}");
    }

    #[test]
    fn surface_interpolates_in_total_variance() {
        let s0 = VolSlice { t: 0.5, forward: 100.0, svi: Svi { a: 0.02, b: 0.1, rho: 0.0, m: 0.0, sigma: 0.1 } };
        let s1 = VolSlice { t: 2.0, forward: 100.0, svi: Svi { a: 0.05, b: 0.1, rho: 0.0, m: 0.0, sigma: 0.1 } };
        let surf = VolSurface { spot: 100.0, reference: 100.0, slices: vec![s0, s1] };
        let w_mid = surf.total_variance_at(1.25, 0.0);
        let expected = 0.5 * (surf.slices[0].svi.total_variance(0.0) + surf.slices[1].svi.total_variance(0.0));
        assert!((w_mid - expected).abs() < 1e-12, "tv interp {w_mid} vs {expected}");
        assert!(surf.arbitrage_violations().is_empty());
    }

    #[test]
    fn calibrate_from_otm_quotes_recovers_slice() {
        let truth = Svi { a: 0.02, b: 0.15, rho: -0.3, m: 0.0, sigma: 0.08 };
        let t = 1.0;
        let spot = 100.0;
        let mut quotes = Vec::new();
        for i in 0..41 {
            let strike = 70.0 + i as f64;
            let k = (strike / spot).ln();
            let iv = truth.vol(k, t);
            quotes.push(Quote { t, strike, iv, is_call: strike > spot, weight: 1.0 });
        }
        let surf = calibrate_quotes(spot, spot, &quotes);
        assert_eq!(surf.slices.len(), 1, "one expiry");
        let max_err = (70..=110)
            .map(|k| {
                let strike = k as f64;
                let kk = (strike / spot).ln();
                (surf.vol_at(strike, t) - truth.vol(kk, t)).abs()
            })
            .fold(0.0, f64::max);
        assert!(max_err < 2e-3, "max vol error {max_err}");
        assert!(surf.arbitrage_violations().is_empty());
    }
}
