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