// stellar_preprocessor/src/pipeline/crossref.rs
//
// Name normalisation and angular separation helpers — used by stage3.

/// Normalise a star or host name into a canonical token for HashMap lookup.
/// Rules (applied in order):
///   1. Lowercase the entire string
///   2. Strip all leading/trailing whitespace
///   3. Collapse all internal whitespace runs to a single space
///   4. Normalise known catalog prefix variants to a canonical form
///
/// Normalised catalog prefixes:
///   "gl "  / "gliese " → "gj "
///   "hd "               → "hd "   (already canonical)
///   "hip "              → "hip "
///   "2mass "            → "2mass "
///   "tic "              → "tic "
///   "tyc "              → "tyc "
///   "wasp-"             → "wasp-"
///   "kepler-"           → "kepler-"
///   "k2-"               → "k2-"
///   "koi-"              → "koi-"
///
/// Examples:
///   "51 Peg"        → "51 peg"
///   "Gliese 876"    → "gj 876"
///   "GL 876"        → "gj 876"
///   "HD  209458"    → "hd 209458"
///   "WASP-39"       → "wasp-39"
///   "Kepler-452"    → "kepler-452"
pub fn normalise_name(name: &str) -> String {
    // Step 1 & 2: lowercase and trim
    let s = name.trim().to_lowercase();

    // Step 3: collapse internal whitespace
    let s: String = s.split_whitespace().collect::<Vec<_>>().join(" ");

    // Step 4: normalise catalog prefix variants
    let s = if s.starts_with("gliese ") {
        format!("gj {}", &s[7..])
    } else if s.starts_with("gl ") {
        format!("gj {}", &s[3..])
    } else {
        s
    };

    s
}

/// Compute the angular separation in arcseconds between two ICRS positions.
/// Uses the haversine formula for numerical stability at small angles.
///
/// Arguments: ra1, dec1, ra2, dec2 — all in degrees.
pub fn angular_separation_arcsec(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    use std::f64::consts::PI;
    let to_rad = |d: f64| d * PI / 180.0;

    let ra1  = to_rad(ra1);
    let dec1 = to_rad(dec1);
    let ra2  = to_rad(ra2);
    let dec2 = to_rad(dec2);

    let d_ra  = (ra2 - ra1) / 2.0;
    let d_dec = (dec2 - dec1) / 2.0;
    let a = d_dec.sin().powi(2) + dec1.cos() * dec2.cos() * d_ra.sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    // Convert radians to arcseconds
    c * (180.0 / PI) * 3600.0
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Name normalisation ────────────────────────────────────────────────

    #[test]
    fn test_normalise_basic_lowercase() {
        assert_eq!(normalise_name("51 Peg"), "51 peg");
    }

    #[test]
    fn test_normalise_gliese_variants() {
        assert_eq!(normalise_name("Gliese 876"), "gj 876");
        assert_eq!(normalise_name("GL 876"),     "gj 876");
        assert_eq!(normalise_name("GJ 876"),     "gj 876");
    }

    #[test]
    fn test_normalise_collapses_whitespace() {
        assert_eq!(normalise_name("HD  209458"), "hd 209458");
        assert_eq!(normalise_name("  WASP-39 "), "wasp-39");
    }

    #[test]
    fn test_normalise_kepler() {
        assert_eq!(normalise_name("Kepler-452"), "kepler-452");
        assert_eq!(normalise_name("K2-18"),      "k2-18");
    }

    #[test]
    fn test_normalise_other_prefixes() {
        assert_eq!(normalise_name("HIP 12345"),    "hip 12345");
        assert_eq!(normalise_name("TIC 350464481"), "tic 350464481");
        assert_eq!(normalise_name("TYC 1234-567-1"), "tyc 1234-567-1");
        assert_eq!(normalise_name("KOI-172"),      "koi-172");
    }

    #[test]
    fn test_normalise_idempotent() {
        let once   = normalise_name("Gliese 876");
        let twice  = normalise_name(&once);
        assert_eq!(once, twice, "normalise_name must be idempotent");
    }

    // ── Angular separation ───────────────────────────────────────────────

    #[test]
    fn test_angular_separation_zero() {
        let sep = angular_separation_arcsec(10.0, 20.0, 10.0, 20.0);
        assert!(sep < 1e-9, "Same point should have zero separation, got {}", sep);
    }

    #[test]
    fn test_angular_separation_one_arcsecond() {
        // A point 1 arcsecond north in declination.
        let sep = angular_separation_arcsec(0.0, 0.0, 0.0, 1.0 / 3600.0);
        assert!(
            (sep - 1.0).abs() < 0.001,
            "Expected ~1 arcsecond, got {}",
            sep
        );
    }

    #[test]
    fn test_angular_separation_symmetric() {
        let sep_ab = angular_separation_arcsec(10.0, 20.0, 10.1, 20.1);
        let sep_ba = angular_separation_arcsec(10.1, 20.1, 10.0, 20.0);
        assert!(
            (sep_ab - sep_ba).abs() < 1e-10,
            "angular_separation must be symmetric"
        );
    }
}