//! Cross-sectional correlation engine for instruments that live in the lake.
//!
//! Computes **Kendall tau-b** between the daily returns of every pair of legs
//! (stock underlyings × FX currency pairs) over a trailing window, then applies
//! the dampening update
//!
//! ```text
//! rho = rho0 + alpha * (1 - rho)
//! ```
//!
//! whose fixed point (the value the recursion converges to) is
//!
//! ```text
//! rho = (rho0 + alpha) / (1 + alpha)
//! ```
//!
//! `alpha` is the dampening weight toward +1 (risk-conservative: a low-return
//! pair default never stays at its raw rho0). The engine is pure math — callers
//! supply the aligned close matrix and consume the packed upper-triangle.
//!
//! Kendall is rank-based (monotone transform invariant), so log/simple returns
//! give identical tau — we use simple percentage returns. The pair loop is
//! parallelised with `rayon` and each pair is O(T log T) (rank + inversion
//! count), which is several orders of magnitude faster than the naive O(T²)
//! concordance pass and keeps a full N≈1600 matrix (~1.3M pairs × 252 rows)
//! in the seconds-to-a-minute range on multi-core hardware.

use ndarray::prelude::*;
use rayon::prelude::*;

/// Percentage return (close_t / close_{t-1} - 1) with `None` where stale/
/// missing. Row alignment is the caller's job; here we just diff in place and
/// drop the leading NaN by shifting (n → n-1).
pub fn pct_returns(closes: &[f64]) -> Vec<f64> {
    if closes.len() < 2 {
        return Vec::new();
    }
    closes
        .iter()
        .zip(closes.iter().skip(1))
        .map(|(&prev, &cur)| if prev > 0.0 { cur / prev - 1.0 } else { f64::NAN })
        .collect()
}

/// Average ranks (ties split) — the standard Kendall/Spearman rank transform.
fn average_rank(data: &[f64]) -> Vec<f64> {
    let mut ranked: Vec<(usize, f64)> = data.iter().copied().enumerate().collect();
    ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranks = vec![0.0; data.len()];
    let mut i = 0;
    while i < ranked.len() {
        let mut run_end = i;
        let mut sum = (i + 1) as f64;
        while run_end + 1 < ranked.len() && ranked[run_end + 1].1 == ranked[i].1 {
            run_end += 1;
            sum += (run_end + 1) as f64;
        }
        let avg = sum / (run_end - i + 1) as f64;
        for r in &ranked[i..=run_end] {
            ranks[r.0] = avg;
        }
        i = run_end + 1;
    }
    ranks
}

/// Number of inversions in `arr` (stable mergesort variant), i.e. pairs i<j
/// with arr[i] > arr[j]. Used for the concordance count of a Kendall pair.
fn count_inversions(arr: &[f64]) -> usize {
    fn merge_sort(a: &mut [f64], tmp: &mut [f64]) -> usize {
        let n = a.len();
        if n <= 1 {
            return 0;
        }
        let mid = n / 2;
        let mut inv = merge_sort(&mut a[..mid], &mut tmp[..mid]);
        inv += merge_sort(&mut a[mid..], &mut tmp[mid..]);
        // merge with inversion counting
        let (mut i, mut j, mut k) = (0, mid, 0);
        while i < mid && j < n {
            if a[i] <= a[j] {
                tmp[k] = a[i];
                i += 1;
            } else {
                tmp[k] = a[j];
                inv += mid - i;
                j += 1;
            }
            k += 1;
        }
        tmp[k..k + (mid - i)].copy_from_slice(&a[i..mid]);
        tmp[k + (mid - i)..k + (mid - i) + (n - j)].copy_from_slice(&a[j..n]);
        a.copy_from_slice(&tmp[..n]);
        inv
    }
    let mut a = arr.to_vec();
    let mut tmp = vec![0.0; a.len()];
    merge_sort(&mut a, &mut tmp)
}

/// Ties per rank signature: Σ over tied groups of t(t−1)/2, and the number of
/// strictly-discordant ordered pairs (inversions) plus y-tied pairs for a
/// sequence slice — all helpers for the tau-b numerator.
fn tie_adjust(ranks: &[f64]) -> f64 {
    let mut sorted = ranks.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));
    let mut adj = 0.0;
    let mut i = 0;
    while i < sorted.len() {
        let mut j = i + 1;
        while j < sorted.len() && sorted[j] == sorted[i] {
            j += 1;
        }
        let k = (j - i) as f64;
        adj += k * (k - 1.0) / 2.0;
        i = j;
    }
    adj
}

fn discord_and_yties(ry: &[f64]) -> (f64, f64) {
    let inv = count_inversions(ry) as f64;
    // y-tied pairs = those with equal rank_y, counted from the sorted slice.
    let mut sorted = ry.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));
    let mut yties = 0.0;
    let mut i = 0;
    while i < sorted.len() {
        let mut j = i + 1;
        while j < sorted.len() && sorted[j] == sorted[i] {
            j += 1;
        }
        let k = (j - i) as f64;
        yties += k * (k - 1.0) / 2.0;
        i = j;
    }
    (inv, yties)
}

/// Kendall tau-b between two return series (lengths equal). Handles ties in
/// both series exactly (scipy-compatible): order points by x-rank, then
/// numerator = C − D over x-strictly-ordered pairs, denominator from x/y tie
/// counts (`n0 − n1`)(`n0 − n2`).
pub fn kendall_tau_b(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }
    let n = x.len() as f64;
    let total = n * (n - 1.0) / 2.0;
    let rx = average_rank(x);
    let ry = average_rank(y);

    let mut pairs: Vec<(f64, f64)> = rx.iter().zip(ry.iter()).map(|(&a, &b)| (a, b)).collect();
    // Stable: ties in x keep original order so x-tie blocks are contiguous.
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    // Numerator over ALL pairs, then subtract the contribution of x-tied
    // blocks (their sign you'd include but tau-b drops them because the
    // x-rank difference is zero).
    let sorted_ry: Vec<f64> = pairs.iter().map(|&(_, ry)| ry).collect();
    let (inv_all, yties_all) = discord_and_yties(&sorted_ry);
    let num_all = total - 2.0 * inv_all - yties_all;

    // x-tie blocks: contiguous runs of equal rx in the sorted order.
    let mut num_xtied = 0.0;
    let mut start = 0;
    while start < pairs.len() {
        let mut end = start + 1;
        while end < pairs.len() && pairs[end].0 == pairs[start].0 {
            end += 1;
        }
        if end - start > 1 {
            let (inv_b, yt_b) = discord_and_yties(&sorted_ry[start..end]);
            let bt = (end - start) as f64 * ((end - start) as f64 - 1.0) / 2.0;
            num_xtied += bt - 2.0 * inv_b - yt_b;
        }
        start = end;
    }
    let num = num_all - num_xtied;

    let n1 = tie_adjust(&rx);
    let n2 = tie_adjust(&ry);
    let denom = ((total - n1) * (total - n2)).max(0.0).sqrt();
    if denom <= 0.0 {
        return None;
    }
    Some(num / denom)
}

/// Dampened correlation: fixed point of `rho = rho0 + alpha*(1 - rho)`.
pub fn dampen(rho0: f64, alpha: f64) -> f64 {
    let a = alpha.clamp(0.0, 1.0);
    (rho0 + a) / (1.0 + a)
}

/// Align a vector of per-leg closes to a single common length using
/// forward-fill of the last known close (leading `NaN` stays `NaN`). All legs
/// must share the same time axis order; missing interior observations are
/// carried forward so the returns matrix stays dense.
pub fn ffill(closes: &[Option<f64>]) -> Vec<Option<f64>> {
    let mut out = Vec::with_capacity(closes.len());
    let mut last: Option<f64> = None;
    for c in closes {
        match c {
            Some(v) if v.is_finite() => {
                last = Some(*v);
                out.push(Some(*v));
            }
            _ => out.push(last),
        }
    }
    out
}

/// Kendall tau-b matrix (upper triangle, packed row-major: all pairs (i,j)
/// with i<j, i iterating outer, j inner) for an `Array2` whose **columns** are
/// the instrument return series (rows = time). Runs the pair loop in parallel.
/// `(i, i)` self-pairs are not emitted.
pub fn packed_kendall_upper(returns: &Array2<f64>) -> Result<Vec<f64>, String> {
    let (t, n) = returns.dim();
    if t < 2 || n < 2 {
        return Err("need at least 2 rows and 2 columns".into());
    }
    let cols: Vec<Vec<f64>> = (0..n).map(|j| returns.column(j).to_vec()).collect();
    let mut pairs_idx: Vec<(usize, usize)> = Vec::with_capacity(n * (n - 1) / 2);
    for i in 0..n {
        for j in (i + 1)..n {
            pairs_idx.push((i, j));
        }
    }
    let out: Vec<f64> = pairs_idx
        .par_iter()
        .map(|&(i, j)| {
            kendall_tau_b(&cols[i], &cols[j]).unwrap_or(f64::NAN)
        })
        .collect();
    Ok(out)
}

/// Expand a packed upper-triangle (from [`packed_kendall_upper`]) into full
/// N×N symmetric labels: returns `(i, j, value)` triples in the same order.
pub fn unpack_pairs(n: usize, packed: &[f64]) -> Vec<(usize, usize, f64)> {
    let mut out = Vec::with_capacity(packed.len());
    let mut k = 0;
    for i in 0..n {
        for j in (i + 1)..n {
            let v = *packed.get(k).unwrap_or(&f64::NAN);
            out.push((i, j, v));
            k += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kendall_perfect_agreement() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let tau = kendall_tau_b(&x, &x).unwrap();
        assert!((tau - 1.0).abs() < 1e-9, "self tau = {tau}");
    }

    #[test]
    fn kendall_perfect_disagreement() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        let y = [5.0, 4.0, 3.0, 2.0, 1.0];
        let tau = kendall_tau_b(&x, &y).unwrap();
        assert!((tau - -1.0).abs() < 1e-9, "reverse tau = {tau}");
    }

    #[test]
    fn kendall_hand_example() {
        // From Wikipedia's tau example: (1..4) vs (2,3,1,4)
        let x = [10.0, 20.0, 30.0, 40.0];
        let y = [20.0, 30.0, 10.0, 40.0];
        let tau = kendall_tau_b(&x, &y).unwrap();
        // pairs: (1,2)c (1,3)d (1,4)c (2,3)d (2,4)c (3,4)c -> 4C,2D -> (4-2)/6
        assert!((tau - 1.0 / 3.0).abs() < 1e-9, "tau = {tau}");
    }

    #[test]
    fn kendall_ties_reduce_denominator() {
        let x = [1.0, 1.0, 2.0, 3.0];
        let y = [1.0, 2.0, 2.0, 3.0];
        let tau = kendall_tau_b(&x, &y).unwrap();
        // n0 = 6; one x tie (tx=1) and one y tie (ty=1) -> denom = sqrt(5*5)=5.
        // Distinct-x / distinct-y concordant pairs: (1,1)-(2,2), (1,1)-(3,3),
        // (1,2)-(3,3), (2,2)-(3,3) -> C=4, D=0 -> tau = 4/5 = 0.8.
        assert!((tau - 0.8).abs() < 1e-9, "tied tau = {tau}");
    }

    #[test]
    fn dampen_fixed_point() {
        // rho = 0, alpha = 0.4 -> (0 + 0.4)/(1.4) = 0.2857
        let d = dampen(0.0, 0.4);
        assert!((d - 0.4 / 1.4).abs() < 1e-9);
        // negative correlation is pulled toward 1 too
        assert!(dampen(-1.0, 0.4) > -1.0);
        // alpha clamps into [0,1] -> alpha=3 behaves as alpha=1
        assert_eq!(dampen(0.5, 3.0), dampen(0.5, 1.0));
    }

    #[test]
    fn packed_matches_brute_force() {
        // 5 time rows, 3 instrument columns (columns = return series).
        let returns = Array2::from_shape_vec((5, 3), vec![
            1.0, 2.0, 3.0, 4.0, 5.0,
            5.0, 4.0, 3.0, 2.0, 1.0,
            1.0, 3.0, 2.0, 5.0, 4.0,
        ]).unwrap();
        let packed = packed_kendall_upper(&returns).unwrap();
        assert_eq!(packed.len(), 3);
        for (i, j, v) in unpack_pairs(3, &packed) {
            let brute = kendall_tau_b(
                &returns.column(i).to_vec(),
                &returns.column(j).to_vec(),
            )
            .unwrap();
            assert!((v - brute).abs() < 1e-12, "pair ({i},{j}) {v} != {brute}");
        }
    }
}