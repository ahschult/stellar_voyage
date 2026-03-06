// stellar_preprocessor/src/pipeline/exoplanet_csv.rs
//
// Parse the NASA Exoplanet Archive Planetary Systems (PS) table CSV into
// a Vec<RawPlanetRecord>.
//
// The PS table CSV begins with a multi-line comment header where every line
// starts with '#'. csv::ReaderBuilder's `.comment(Some(b'#'))` silently skips
// these lines. Verify that your downloaded file actually uses '#' for comment
// lines by opening the raw CSV in a text editor and checking the first 10 lines.

use crate::types::RawPlanetRecord;
use anyhow::{bail, Context, Result};
use std::path::Path;

/// Required column names in the NASA Exoplanet Archive PS table.
/// These are the default column names from the TAP/ADQL CSV download.
const COL_PLANET_NAME:   &str = "pl_name";
const COL_HOSTNAME:      &str = "hostname";
const COL_RA:            &str = "ra";
const COL_DEC:           &str = "dec";
const COL_ORBSMAX:       &str = "pl_orbsmax";
const COL_ORBPER:        &str = "pl_orbper";
const COL_RADE:          &str = "pl_rade";
const COL_EQT:           &str = "pl_eqt";
const COL_DISCMETHOD:    &str = "discoverymethod";

pub fn parse(path: &Path) -> Result<Vec<RawPlanetRecord>> {
    let mut reader = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .trim(csv::Trim::All)
        .flexible(false)
        .from_path(path)
        .with_context(|| format!("Failed to open exoplanet CSV: {:?}", path))?;

    // ── Locate required column indices ───────────────────────────────────
    let headers = reader
        .headers()
        .with_context(|| "Failed to read CSV headers")?
        .clone();

    let find = |col: &str| -> Result<usize> {
        headers
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case(col))
            .with_context(|| {
                format!(
                    "Required column '{}' not found in exoplanet CSV. Headers: {:?}",
                    col,
                    headers.iter().collect::<Vec<_>>()
                )
            })
    };

    let idx_name       = find(COL_PLANET_NAME)?;
    let idx_hostname   = find(COL_HOSTNAME)?;
    let idx_ra         = find(COL_RA)?;
    let idx_dec        = find(COL_DEC)?;
    let idx_orbsmax    = find(COL_ORBSMAX)?;
    let idx_orbper     = find(COL_ORBPER)?;
    let idx_rade       = find(COL_RADE)?;
    let idx_eqt        = find(COL_EQT)?;
    let idx_discmethod = find(COL_DISCMETHOD)?;

    // ── Parse rows ───────────────────────────────────────────────────────
    let mut records = Vec::new();
    let mut skipped = 0usize;

    for result in reader.records() {
        let row = match result {
            Ok(r)  => r,
            Err(_) => { skipped += 1; continue; }
        };

        // Required numeric fields — skip row on parse failure.
        let planet_name = row[idx_name].trim().to_string();
        let hostname    = row[idx_hostname].trim().to_string();

        if planet_name.is_empty() || hostname.is_empty() {
            skipped += 1;
            continue;
        }

        let ra  = match row[idx_ra].trim().parse::<f64>()  { Ok(v) => v, Err(_) => { skipped += 1; continue; } };
        let dec = match row[idx_dec].trim().parse::<f64>() { Ok(v) => v, Err(_) => { skipped += 1; continue; } };

        // semi_major_au and period_days are required for astrophysical inference;
        // skip planets where both are absent.
        let semi_major_au = match row[idx_orbsmax].trim().parse::<f32>() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };
        let period_days = match row[idx_orbper].trim().parse::<f32>() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };

        // Optional fields: absent is expected for RV-only detections.
        let radius_earth   = row[idx_rade].trim().parse::<f32>().ok();
        let eq_temp_kelvin = row[idx_eqt].trim().parse::<f32>().ok();
        let discovery_method = row[idx_discmethod].trim().to_string();

        records.push(RawPlanetRecord {
            planet_name,
            hostname,
            ra,
            dec,
            semi_major_au,
            period_days,
            radius_earth,
            eq_temp_kelvin,
            discovery_method,
        });
    }

    if records.is_empty() {
        bail!(
            "Exoplanet CSV parsed zero valid records. \
             Check column names and file path: {:?}",
            path
        );
    }

    println!(
        "  Exoplanet CSV: {} planets parsed, {} skipped",
        records.len(),
        skipped
    );

    Ok(records)
}