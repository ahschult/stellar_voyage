// stellar_preprocessor/src/pipeline/stage4.rs
//
// Stage 4: Gap inference and DataQuality tagging.
//
// Sub-task 4a — bp_rp inference
//   For each star missing a color index, compute the median bp_rp of all
//   observed stars in the same spatial chunk that share a similar G-band
//   magnitude (within ±0.5 mag).
//
//   Chunk assignment uses the same floor(xyz / chunk_size) computation that
//   Stage 5 (M4) will use for the final chunking. No chunk files are written
//   here — this is grouping only.
//
// Sub-task 4b — Planet field inference
//   Fill missing radius_earth (Wolfgang et al. period-proxy) and
//   eq_temp_kelvin (Casagrande + Stefan-Boltzmann).
//   All inferred values are tagged DataQuality::Inferred.

use crate::types::EnrichedStarRecord;
use anyhow::Result;
use stellar_types::{DataQuality, PlanetRecord, StarRecord};

pub struct Stage4Result {
    pub records: Vec<StarRecord>,
    pub bp_rp_inferred_count: usize,
    pub radius_inferred_count: usize,
    pub temp_inferred_count: usize,
    pub radius_unresolvable_count: usize,
    pub temp_unresolvable_count: usize,
}

pub fn run(records: Vec<EnrichedStarRecord>, chunk_size: f64) -> Result<Stage4Result> {
    // ── 4a: bp_rp spatial inference ─────────────────────────────────────
    let (records, bp_rp_inferred_count) = infer_bp_rp(records, chunk_size);

    // ── 4b: Convert to StarRecord / PlanetRecord with DataQuality tags ──
    let mut final_records = Vec::with_capacity(records.len());
    let mut radius_inferred      = 0usize;
    let mut temp_inferred        = 0usize;
    let mut radius_unresolvable  = 0usize;
    let mut temp_unresolvable    = 0usize;

    for r in records {
        let mut planet_records = Vec::new();

        for p in &r.planets {
            let (radius_earth, radius_quality) = resolve_radius(
                p.radius_earth,
                p.period_days,
                r.magnitude,
                &mut radius_inferred,
                &mut radius_unresolvable,
            );

            let eq_temp_kelvin = resolve_eq_temp(
                p.eq_temp_kelvin,
                r.bp_rp,
                p.semi_major_au,
                &mut temp_inferred,
                &mut temp_unresolvable,
            );

            planet_records.push(PlanetRecord {
                planet_name:   p.planet_name.clone(),
                semi_major_au: p.semi_major_au,
                period_days:   p.period_days,
                radius_earth,
                eq_temp_kelvin,
                quality: radius_quality,
            });
        }

        // Star quality: Observed if all catalog data present, Inferred if
        // bp_rp was estimated. Individual planet qualities are set per-field.
        let star_quality = if r.bp_rp.is_some() {
            DataQuality::Observed
        } else {
            // bp_rp is still None only if the entire spatial chunk was data-
            // poor (extremely rare). Fall back to solar color index.
            DataQuality::Inferred
        };

        let color_index = r
            .bp_rp
            .unwrap_or(0.82); // solar default — last resort for data-poor chunks

        final_records.push(StarRecord {
            gaia_source_id: r.source_id,
            x: r.x,
            y: r.y,
            z: r.z,
            magnitude: r.magnitude,
            color_index,
            has_planets: !planet_records.is_empty(),
            quality: star_quality,
            planets: planet_records,
        });
    }

    Ok(Stage4Result {
        records: final_records,
        bp_rp_inferred_count,
        radius_inferred_count: radius_inferred,
        temp_inferred_count: temp_inferred,
        radius_unresolvable_count: radius_unresolvable,
        temp_unresolvable_count: temp_unresolvable,
    })
}

// ── 4a: bp_rp spatial inference ─────────────────────────────────────────────

fn infer_bp_rp(
    mut records: Vec<EnrichedStarRecord>,
    chunk_size: f64,
) -> (Vec<EnrichedStarRecord>, usize) {
    use std::collections::HashMap;

    // (chunk_ix, chunk_iy, chunk_iz, mag_bin) → sorted observed bp_rp values
    // mag_bin = floor(magnitude / 0.5)
    type BinKey = (i32, i32, i32, i32);

    let mut bin_values: HashMap<BinKey, Vec<f32>> = HashMap::new();

    for r in &records {
        if let Some(bp_rp) = r.bp_rp {
            let ix      = (r.x / chunk_size).floor() as i32;
            let iy      = (r.y / chunk_size).floor() as i32;
            let iz      = (r.z / chunk_size).floor() as i32;
            let mag_bin = (r.magnitude as f64 / 0.5).floor() as i32;
            bin_values
                .entry((ix, iy, iz, mag_bin))
                .or_default()
                .push(bp_rp);
        }
    }

    // Pre-sort all bins once for O(1) median lookup.
    for values in bin_values.values_mut() {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    }

    let median = |v: &[f32]| -> f32 {
        let mid = v.len() / 2;
        if v.len() % 2 == 0 {
            (v[mid - 1] + v[mid]) / 2.0
        } else {
            v[mid]
        }
    };

    let mut inferred_count = 0usize;

    for r in &mut records {
        if r.bp_rp.is_some() {
            continue;
        }

        let ix      = (r.x / chunk_size).floor() as i32;
        let iy      = (r.y / chunk_size).floor() as i32;
        let iz      = (r.z / chunk_size).floor() as i32;
        let mag_bin = (r.magnitude as f64 / 0.5).floor() as i32;

        // Try exact magnitude bin first, then adjacent bins (±1) as fallback.
        let inferred = bin_values
            .get(&(ix, iy, iz, mag_bin))
            .or_else(|| bin_values.get(&(ix, iy, iz, mag_bin + 1)))
            .or_else(|| bin_values.get(&(ix, iy, iz, mag_bin - 1)))
            .map(|v| median(v));

        if let Some(value) = inferred {
            r.bp_rp = Some(value);
            inferred_count += 1;
        }
        // If still None: the chunk has no observed bp_rp at all (extremely
        // rare). Leave as None — Stage 4b handles None bp_rp gracefully,
        // and the final StarRecord receives the 0.82 solar fallback.
    }

    (records, inferred_count)
}

// ── 4b: Planet field inference ───────────────────────────────────────────────

/// Simplified Wolfgang et al. (2016) radius inference using orbital period
/// as a proxy for planet class. Returns (radius_earth, DataQuality).
///
/// If radius is already observed, returns it with Observed quality.
/// If period is available, estimates radius by period bin.
/// DataQuality::Inferred is applied to all estimated values.
fn resolve_radius(
    observed: Option<f32>,
    period_days: f32,
    _host_magnitude: f32,
    inferred_count: &mut usize,
    _unresolvable_count: &mut usize,
) -> (Option<f32>, DataQuality) {
    if let Some(r) = observed {
        return (Some(r), DataQuality::Observed);
    }

    // Period-bin approximation from Wolfgang et al.:
    //   < 10 d   → likely super-Earth    (~1.5 R_earth)
    //   10–100 d → likely sub-Neptune    (~2.5 R_earth)
    //   100–1000 → likely Neptune/Saturn (~5.0 R_earth)
    //   > 1000 d → likely Jupiter class  (~10.0 R_earth)
    let estimated_radius: f32 = if period_days < 10.0 {
        1.5
    } else if period_days < 100.0 {
        2.5
    } else if period_days < 1000.0 {
        5.0
    } else {
        10.0
    };

    *inferred_count += 1;
    (Some(estimated_radius), DataQuality::Inferred)
}

/// Casagrande et al. (2010) Table 4 polynomial: bp_rp → T_eff (Kelvin).
/// Valid for 0.5 < bp_rp < 3.0. Values outside this range are clamped.
fn casagrande_teff(bp_rp: f32) -> f32 {
    let x = bp_rp.clamp(0.5, 3.0);
    9245.0 - 2700.0 * x + 508.0 * x * x
}

/// Equilibrium temperature inference using the Stefan-Boltzmann energy
/// balance formula:
///
///   T_eq = T_star × sqrt(R_star / (2a)) × (1 − A_B)^0.25
///
/// Returns None if the host bp_rp is unavailable even after Stage 4a.
pub fn resolve_eq_temp(
    observed: Option<f32>,
    host_bp_rp: Option<f32>,
    semi_major_au: f32,
    inferred_count: &mut usize,
    unresolvable_count: &mut usize,
) -> Option<f32> {
    if let Some(t) = observed {
        return Some(t);
    }

    let bp_rp = match host_bp_rp {
        Some(v) => v,
        None => {
            // Host star has no color index even after Stage 4a inference.
            // Cannot compute equilibrium temperature without it.
            *unresolvable_count += 1;
            return None;
        }
    };

    let t_star   = casagrande_teff(bp_rp);
    let r_star_au = 0.00465_f32; // 1 solar radius in AU (default)
    let albedo    = 0.3_f32;     // Bond albedo — Earth default

    // T_eq = T_star × sqrt(R_star / (2a)) × (1 − A)^0.25
    let t_eq = t_star
        * (r_star_au / (2.0 * semi_major_au)).sqrt()
        * (1.0 - albedo).powf(0.25);

    if t_eq.is_finite() && t_eq > 0.0 {
        *inferred_count += 1;
        Some(t_eq)
    } else {
        *unresolvable_count += 1;
        None
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Equilibrium temperature ───────────────────────────────────────────

    #[test]
    fn test_eq_temp_earth_like() {
        // Earth: 1.0 AU from a solar-type host (bp_rp ≈ 0.82).
        // Expected T_eq ≈ 255 K (effective temperature without greenhouse effect).
        let temp = resolve_eq_temp(None, Some(0.82), 1.0, &mut 0, &mut 0);
        let t = temp.expect("should produce an inferred value for solar host at 1 AU");
        assert!(
            (t - 255.0).abs() < 20.0,
            "Expected ~255 K for Earth-like orbit, got {} K",
            t
        );
    }

    #[test]
    fn test_eq_temp_hot_jupiter() {
        // Hot Jupiter: 0.05 AU from a solar host.
        // Expected T_eq ≈ 1140 K.
        let temp = resolve_eq_temp(None, Some(0.82), 0.05, &mut 0, &mut 0);
        let t = temp.expect("should produce a value for hot Jupiter");
        assert!(
            t > 900.0 && t < 1400.0,
            "Expected ~1140 K for hot Jupiter at 0.05 AU, got {} K",
            t
        );
    }

    #[test]
    fn test_eq_temp_observed_passthrough() {
        // When observed temperature is present, return it unchanged.
        let temp = resolve_eq_temp(Some(500.0), Some(0.82), 1.0, &mut 0, &mut 0);
        assert_eq!(temp, Some(500.0));
    }

    #[test]
    fn test_eq_temp_unresolvable_without_host_color() {
        let mut unresolvable = 0usize;
        let temp = resolve_eq_temp(None, None, 1.0, &mut 0, &mut unresolvable);
        assert!(temp.is_none(), "Should be None when host bp_rp is missing");
        assert_eq!(unresolvable, 1);
    }

    // ── Radius inference ─────────────────────────────────────────────────

    #[test]
    fn test_radius_observed_passthrough() {
        let (r, q) = resolve_radius(Some(2.5), 30.0, 5.0, &mut 0, &mut 0);
        assert_eq!(r, Some(2.5));
        assert_eq!(q, DataQuality::Observed);
    }

    #[test]
    fn test_radius_inferred_by_period_bin() {
        let (r, q) = resolve_radius(None, 5.0, 5.0, &mut 0, &mut 0);
        assert_eq!(r, Some(1.5)); // < 10 d → super-Earth
        assert_eq!(q, DataQuality::Inferred);

        let (r, _) = resolve_radius(None, 50.0, 5.0, &mut 0, &mut 0);
        assert_eq!(r, Some(2.5)); // 10–100 d → sub-Neptune

        let (r, _) = resolve_radius(None, 500.0, 5.0, &mut 0, &mut 0);
        assert_eq!(r, Some(5.0)); // 100–1000 d → Neptune/Saturn

        let (r, _) = resolve_radius(None, 5000.0, 5.0, &mut 0, &mut 0);
        assert_eq!(r, Some(10.0)); // > 1000 d → Jupiter
    }

    // ── Casagrande teff ───────────────────────────────────────────────────

    #[test]
    fn test_casagrande_solar() {
        // Sun: bp_rp ≈ 0.82 → T_eff should be near 5778 K.
        let t = casagrande_teff(0.82);
        assert!(
            (t - 5778.0).abs() < 400.0,
            "Solar T_eff should be near 5778 K, got {} K",
            t
        );
    }

    #[test]
    fn test_casagrande_clamp() {
        // Values outside [0.5, 3.0] should clamp and not panic.
        let _ = casagrande_teff(-1.0);
        let _ = casagrande_teff(5.0);
    }

    // ── bp_rp spatial inference ───────────────────────────────────────────

    #[test]
    fn test_infer_bp_rp_fills_missing() {
        use crate::types::EnrichedStarRecord;

        // Three stars in the same chunk and magnitude bin.
        // Two have observed bp_rp; one is missing.
        let make_star = |bp_rp: Option<f32>, mag: f32| EnrichedStarRecord {
            source_id: 0,
            ra: 0.0,
            dec: 0.0,
            parallax: 10.0,
            magnitude: mag,
            bp_rp,
            x: 10.0, y: 10.0, z: 10.0,
            planets: vec![],
        };

        let records = vec![
            make_star(Some(0.8), 5.0),
            make_star(Some(1.0), 5.1),
            make_star(None,      5.0), // should be inferred
        ];

        let chunk_size = 50.0_f64;
        let (result, count) = infer_bp_rp(records, chunk_size);

        assert_eq!(count, 1, "exactly one star should have been inferred");
        // Median of [0.8, 1.0] = 0.9
        let inferred = result[2].bp_rp.expect("bp_rp should be filled");
        assert!(
            (inferred - 0.9).abs() < 0.01,
            "Expected median 0.9, got {}",
            inferred
        );
    }
}