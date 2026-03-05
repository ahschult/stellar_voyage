// stellar_preprocessor/src/pipeline/stage2.rs
//
// Stage 2 — ICRS (ra, dec, parallax) → Cartesian (x, y, z) in parsecs.
//
// Formula (§4.2 of architecture doc):
//
//   d (pc)   = 1000.0 / parallax_mas
//   ra_rad   = ra  * π / 180
//   dec_rad  = dec * π / 180
//   x = d * cos(dec_rad) * cos(ra_rad)
//   y = d * cos(dec_rad) * sin(ra_rad)
//   z = d * sin(dec_rad)
//
// Sol is at (0, 0, 0). The galactic centre is approximately at
// (−8178, 0, 17) parsecs in this frame.
//
// This stage is embarrassingly parallel — each record's conversion depends
// only on its own fields. Uses rayon for performance.
//
// Stage 2 cannot fail on valid Stage 1 output: all records that reach here
// have parallax > 0 (guaranteed by Stage 1 Filter 1).

use crate::types::RawStarRecord;
use rayon::prelude::*;
use std::f64::consts::PI;

/// Convert all records' (ra, dec, parallax) → (x, y, z) in-place.
///
/// Panics if any record has `parallax <= 0` (Stage 1 invariant violation).
pub fn run(records: &mut Vec<RawStarRecord>) {
    records.par_iter_mut().for_each(|r| {
        debug_assert!(
            r.parallax > 0.0,
            "Stage 2 received record with non-positive parallax (source_id={}). \
             Stage 1 filter was not applied correctly.",
            r.source_id
        );

        let d = 1000.0 / r.parallax;
        let ra_rad = r.ra * PI / 180.0;
        let dec_rad = r.dec * PI / 180.0;

        r.x = Some(d * dec_rad.cos() * ra_rad.cos());
        r.y = Some(d * dec_rad.cos() * ra_rad.sin());
        r.z = Some(d * dec_rad.sin());
    });
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_raw(ra: f64, dec: f64, parallax: f64) -> RawStarRecord {
        RawStarRecord {
            source_id: 0,
            ra,
            dec,
            parallax,
            parallax_error: parallax / 10.0, // SNR = 10 — always passes
            magnitude: 5.0,
            bp_rp: None,
            x: None,
            y: None,
            z: None,
        }
    }

    fn distance(r: &RawStarRecord) -> f64 {
        let x = r.x.unwrap();
        let y = r.y.unwrap();
        let z = r.z.unwrap();
        (x * x + y * y + z * z).sqrt()
    }

    // ── Trivial cases ─────────────────────────────────────────────────────

    #[test]
    fn sol_direction_ra0_dec0() {
        // A star at ra=0, dec=0, parallax=1000 mas should land at x=1.0, y=0, z=0.
        let mut records = vec![make_raw(0.0, 0.0, 1000.0)];
        run(&mut records);
        let r = &records[0];
        assert!(
            (r.x.unwrap() - 1.0).abs() < 1e-10,
            "x={} expected 1.0",
            r.x.unwrap()
        );
        assert!(r.y.unwrap().abs() < 1e-10, "y should be ~0");
        assert!(r.z.unwrap().abs() < 1e-10, "z should be ~0");
    }

    #[test]
    fn north_pole_dec90() {
        // dec=90° → star is directly north → x≈0, y≈0, z=d.
        let mut records = vec![make_raw(0.0, 90.0, 1000.0)];
        run(&mut records);
        let r = &records[0];
        assert!(r.x.unwrap().abs() < 1e-10, "x should be ~0 at north pole");
        assert!(r.y.unwrap().abs() < 1e-10, "y should be ~0 at north pole");
        assert!(
            (r.z.unwrap() - 1.0).abs() < 1e-10,
            "z={} expected 1.0",
            r.z.unwrap()
        );
    }

    // ── Known nearby stars (J2000 ICRS catalog values) ────────────────────
    //
    // Validation reference: Simbad / Hipparcos catalog.
    // Tolerance is loose enough to accommodate slightly different catalog
    // editions but tight enough to catch radian/degree bugs.

    #[test]
    fn alpha_centauri_a_distance() {
        // RA=219.8996°, Dec=−60.8335°, parallax=747.10 mas → d≈1.338 pc
        let mut records = vec![make_raw(219.8996, -60.8335, 747.10)];
        run(&mut records);
        let d = distance(&records[0]);
        assert!(
            (d - 1.338).abs() < 0.05,
            "Alpha Cen A: expected ~1.338 pc, got {:.4}",
            d
        );
    }

    #[test]
    fn sirius_distance() {
        // RA=101.2872°, Dec=−16.7161°, parallax=379.21 mas → d≈2.637 pc
        let mut records = vec![make_raw(101.2872, -16.7161, 379.21)];
        run(&mut records);
        let d = distance(&records[0]);
        assert!(
            (d - 2.637).abs() < 0.05,
            "Sirius: expected ~2.637 pc, got {:.4}",
            d
        );
    }

    #[test]
    fn vega_distance() {
        // RA=279.2347°, Dec=+38.7837°, parallax=130.23 mas → d≈7.680 pc
        let mut records = vec![make_raw(279.2347, 38.7837, 130.23)];
        run(&mut records);
        let d = distance(&records[0]);
        assert!(
            (d - 7.680).abs() < 0.1,
            "Vega: expected ~7.680 pc, got {:.4}",
            d
        );
    }

    #[test]
    fn betelgeuse_distance() {
        // RA=88.7929°, Dec=+7.4071°, parallax=4.51 mas → d≈221.7 pc
        let mut records = vec![make_raw(88.7929, 7.4071, 4.51)];
        run(&mut records);
        let d = distance(&records[0]);
        assert!(
            (d - 221.7).abs() < 5.0,
            "Betelgeuse: expected ~221.7 pc, got {:.2}",
            d
        );
    }

    #[test]
    fn barnards_star_distance() {
        // RA=269.4521°, Dec=+4.6933°, parallax=546.98 mas → d≈1.828 pc
        let mut records = vec![make_raw(269.4521, 4.6933, 546.98)];
        run(&mut records);
        let d = distance(&records[0]);
        assert!(
            (d - 1.828).abs() < 0.05,
            "Barnard's Star: expected ~1.828 pc, got {:.4}",
            d
        );
    }

    // ── Parallelism: large batch processes without races ──────────────────

    #[test]
    fn large_batch_processes_correctly() {
        let mut records: Vec<_> = (1..=1000)
            .map(|i| make_raw(i as f64 * 0.1, 0.0, 100.0))
            .collect();
        run(&mut records);
        // All records should now have coordinates populated.
        assert!(records.iter().all(|r| r.x.is_some() && r.y.is_some() && r.z.is_some()));
        // All should be at distance 10 pc (1000/100 = 10).
        for r in &records {
            let d = distance(r);
            assert!((d - 10.0).abs() < 1e-8, "expected d=10.0, got {}", d);
        }
    }
}