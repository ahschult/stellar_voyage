// stellar_preprocessor/src/types.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawStarRecord {
    pub source_id: u64,
    pub ra: f64,           // degrees
    pub dec: f64,          // degrees
    pub parallax: f64,     // milliarcseconds
    pub parallax_error: f64,
    pub magnitude: f32,    // Gaia G-band (phot_g_mean_mag)
    pub bp_rp: Option<f32>, // None if absent in source row
    // Populated after Stage 2:
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub z: Option<f64>,
}

/// A planet record as parsed from the NASA Exoplanet Archive CSV.
/// Held in memory during pipeline processing before becoming a PlanetRecord.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawPlanetRecord {
    pub planet_name: String,
    pub hostname: String,
    pub ra: f64,                      // degrees — used for Pass 2 coordinate match
    pub dec: f64,
    pub semi_major_au: f32,           // pl_orbsmax
    pub period_days: f32,             // pl_orbper
    pub radius_earth: Option<f32>,    // pl_rade — None if absent
    pub eq_temp_kelvin: Option<f32>,  // pl_eqt  — None if absent
    pub discovery_method: String,     // for logging/diagnostics
}

/// The result of enriching a RawStarRecord after Stage 3.
/// Carries matched planet data and is consumed by Stage 4.
///
/// `x`, `y`, `z` are non-Option here — by Stage 3, all coordinates have been
/// populated by Stage 2. Unwrapping from RawStarRecord's Option<f64> at the
/// Stage 2 → 3 boundary makes the invariant explicit in the type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedStarRecord {
    pub source_id: u64,
    pub ra: f64,
    pub dec: f64,
    pub parallax: f64,
    pub magnitude: f32,
    pub bp_rp: Option<f32>,   // still Option — Stage 4 fills missing values
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub planets: Vec<RawPlanetRecord>,
}

pub struct ChunkedStar {
    pub chunk_id: stellar_types::ChunkId,
    pub record: stellar_types::StarRecord,
}