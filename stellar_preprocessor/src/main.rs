// stellar_preprocessor/src/main.rs


mod checkpoint;
mod error;
mod pipeline;
mod types;

use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use stellar_types::{CatalogManifest, DataQuality, StarRecord};
use types::RawStarRecord;

#[derive(Parser, Debug)]
#[command(name = "stellar_preprocessor")]
pub struct Args {
    #[arg(long)]
    pub gaia_path: PathBuf,

    #[arg(long)]
    pub output_path: PathBuf,

    /// Optional path to NASA Exoplanet Archive PS table CSV.
    /// If absent, Stages 3 and 4 are skipped (M2 development workflow).
    #[arg(long)]
    pub exoplanet_path: Option<PathBuf>,

    /// Minimum parallax SNR for Stage 1 filter (passed through to stage1::run).
    #[arg(long, default_value_t = 5.0)]
    pub parallax_snr: f64,

    /// Angular separation threshold (arcseconds) for Pass 2 proximity match.
    #[arg(long, default_value_t = 2.0)]
    pub proximity_arcsec: f64,

    /// G-band magnitude tolerance for Pass 2 proximity match.
    #[arg(long, default_value_t = 1.0)]
    pub proximity_mag_tolerance: f64,

    /// Spatial chunk size in parsecs (Stage 5 and bp_rp inference grouping).
    #[arg(long, default_value_t = 50.0)]
    pub chunk_size: f64,

    /// Resume pipeline from the last completed checkpoint.
    #[arg(long, default_value_t = false)]
    pub resume: bool,

    /// Print per-stage statistics after each stage completes.
    #[arg(long, default_value_t = false)]
    pub stats: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.output_path)?;
    let mut error_log = error::ErrorLog::open(&args.output_path)?;

    // ── Stage 1: Ingest ──────────────────────────────────────────────────
    let stage1_checkpoint = args.output_path.join("stage1.bin");

    let mut raw_records: Vec<RawStarRecord> =
        if args.resume && checkpoint::stage_complete(&args.output_path, "stage1") {
            println!("Stage 1: resuming from checkpoint");
            load_bincode_checkpoint(&stage1_checkpoint)?
        } else {
            println!("Stage 1: ingesting Gaia source...");
            let pb = progress_bar("Ingesting");
            let result = pipeline::stage1::run(
                &args.gaia_path,
                args.parallax_snr,
                &mut error_log,
            )?;
            pb.finish_with_message(format!(
                "{} records passed ({} skipped)",
                result.records.len(),
                result.skipped_rows
            ));
            save_bincode_checkpoint(&stage1_checkpoint, &result.records)?;
            checkpoint::mark_complete(&args.output_path, "stage1")?;
            result.records
        };

    // ── Stage 2: Coordinate Conversion ──────────────────────────────────
    // stage2::run mutates raw_records in-place (no return value).
    // The stage1.bin checkpoint stores pre-Stage-2 records, so --resume
    // reloads from there and re-runs Stage 2 (pure math, negligible cost).
    if !args.resume || !checkpoint::stage_complete(&args.output_path, "stage2") {
        println!("Stage 2: converting coordinates...");
        let pb = progress_bar("Converting");
        pipeline::stage2::run(&mut raw_records);
        pb.finish_with_message("done");
        checkpoint::mark_complete(&args.output_path, "stage2")?;
    } else {
        println!("Stage 2: resuming from checkpoint (re-running conversion)");
        pipeline::stage2::run(&mut raw_records);
    }

    if args.stats {
        let with_color = raw_records.iter().filter(|r| r.bp_rp.is_some()).count();
        println!("── Stage 1+2 Stats ─────────────────────────");
        println!("  Total passing records : {}", raw_records.len());
        println!(
            "  Records with bp_rp    : {} ({:.1}%)",
            with_color,
            100.0 * with_color as f64 / raw_records.len().max(1) as f64
        );
        println!("  Error log entries     : {}", error_log.count);
    }

    error_log.flush();

    // ── Stages 3 & 4: Cross-reference and gap inference ─────────────────
    let final_records: Vec<StarRecord> =
        if let Some(ref exo_path) = args.exoplanet_path {

            // ── Stage 3: Cross-Reference ─────────────────────────────────
            let stage3_checkpoint = args.output_path.join("stage3.bin");

            let enriched = if args.resume
                && checkpoint::stage_complete(&args.output_path, "stage3")
            {
                println!("Stage 3: resuming from checkpoint");
                load_bincode_checkpoint(&stage3_checkpoint)?
            } else {
                println!("Stage 3: cross-referencing exoplanet archive...");
                let pb = progress_bar("Cross-referencing");
                let result = pipeline::stage3::run(
                    raw_records,
                    exo_path,
                    args.proximity_arcsec,
                    args.proximity_mag_tolerance,
                    &args.output_path,
                )?;
                pb.finish_with_message(format!(
                    "pass1={} pass2={} unmatched={}",
                    result.matched_pass1, result.matched_pass2, result.unmatched
                ));
                println!(
                    "  Matched: {} pass-1, {} pass-2 | Unmatched: {} | Total planets: {}",
                    result.matched_pass1,
                    result.matched_pass2,
                    result.unmatched,
                    result.total_planets
                );
                save_bincode_checkpoint(&stage3_checkpoint, &result.records)?;
                checkpoint::mark_complete(&args.output_path, "stage3")?;
                result.records
            };

            // ── Stage 4: Gap Inference ────────────────────────────────────
            let stage4_checkpoint = args.output_path.join("stage4.bin");

            let records: Vec<StarRecord> = if args.resume
                && checkpoint::stage_complete(&args.output_path, "stage4")
            {
                println!("Stage 4: resuming from checkpoint");
                load_bincode_checkpoint(&stage4_checkpoint)?
            } else {
                println!("Stage 4: inferring missing fields and tagging DataQuality...");
                let pb = progress_bar("Inferring");
                let result = pipeline::stage4::run(enriched, args.chunk_size)?;
                pb.finish_with_message("done");
                if args.stats {
                    println!("── Stage 4 Stats ───────────────────────────");
                    println!("  bp_rp inferred      : {}", result.bp_rp_inferred_count);
                    println!("  radius inferred     : {}", result.radius_inferred_count);
                    println!("  temp inferred       : {}", result.temp_inferred_count);
                    println!("  radius unresolvable : {}", result.radius_unresolvable_count);
                    println!("  temp unresolvable   : {}", result.temp_unresolvable_count);
                }
                save_bincode_checkpoint(&stage4_checkpoint, &result.records)?;
                checkpoint::mark_complete(&args.output_path, "stage4")?;
                result.records
            };

            records

        } else {
            println!("Stage 3 & 4: --exoplanet-path not provided; skipping.");
            convert_to_star_records_no_planets(raw_records)
        };

    // ── Stage 5: Chunk Assignment ──────────────────────────────────────────
    println!("Stage 5: assigning stars to spatial chunks...");
    let stage5_result = pipeline::stage5::run(final_records, args.chunk_size);
    println!(
        "  {} stars assigned to {} chunks",
        stage5_result.chunked.len(),
        stage5_result.chunk_count
    );
    checkpoint::mark_complete(&args.output_path, "stage5")?;

    // ── Stage 6: rkyv Serialisation ────────────────────────────────────────
    if !args.resume || !checkpoint::stage_complete(&args.output_path, "stage6") {
        println!("Stage 6: serialising chunks to rkyv binary files...");
        let total_stars = stage5_result.chunked.len() as u64;
        let stage6_result = pipeline::stage6::run(
            stage5_result.chunked,
            &args.output_path,
            args.chunk_size,
            total_stars,
        )?;
        println!(
            "  {} chunk files written, manifest at {}/catalog_manifest.json",
            stage6_result.files_written,
            args.output_path.display(),
        );
        checkpoint::mark_complete(&args.output_path, "stage6")?;

        if args.stats {
            print_manifest_stats(&stage6_result.manifest);
        }
    } else {
        println!("Stage 6: resuming from checkpoint (chunk files already written)");
    }

    println!("\nDone.");
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn save_bincode_checkpoint<T: serde::Serialize>(
    path: &std::path::Path,
    data: &T,
) -> Result<()> {
    let bytes = bincode::serialize(data)?;
    checkpoint::write_atomic(path, &bytes)?;
    Ok(())
}

fn load_bincode_checkpoint<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
) -> Result<T> {
    let bytes = std::fs::read(path)?;
    Ok(bincode::deserialize(&bytes)?)
}

/// Convert raw records to StarRecord with no planet data.
/// Used when --exoplanet-path is absent. Stars missing bp_rp receive the
/// solar fallback (0.82) tagged DataQuality::Inferred.
fn convert_to_star_records_no_planets(records: Vec<RawStarRecord>) -> Vec<StarRecord> {
    records
        .into_iter()
        .map(|r| {
            let (color_index, quality) = match r.bp_rp {
                Some(v) => (v, DataQuality::Observed),
                None    => (0.82_f32, DataQuality::Inferred),
            };
            StarRecord {
                gaia_source_id: r.source_id,
                x: r.x.unwrap_or(0.0),
                y: r.y.unwrap_or(0.0),
                z: r.z.unwrap_or(0.0),
                magnitude: r.magnitude,
                color_index,
                has_planets: false,
                quality,
                planets: vec![],
            }
        })
        .collect()
}

fn print_manifest_stats(manifest: &CatalogManifest) {
    println!("── Manifest Stats ───────────────────────────────");
    println!("  Total stars     : {}", manifest.total_stars);
    println!("  Total chunks    : {}", manifest.chunks.len());
    println!("  Chunk size (pc) : {}", manifest.chunk_size_parsecs);

    let total_observed:  u32 = manifest.chunks.iter().map(|c| c.observed_count).sum();
    let total_inferred:  u32 = manifest.chunks.iter().map(|c| c.inferred_count).sum();
    let total_synthetic: u32 = manifest.chunks.iter().map(|c| c.synthetic_count).sum();
    let total = manifest.total_stars as f64;

    println!(
        "  Observed  : {} ({:.1}%)",
        total_observed,
        100.0 * total_observed as f64 / total
    );
    println!(
        "  Inferred  : {} ({:.1}%)",
        total_inferred,
        100.0 * total_inferred as f64 / total
    );
    println!(
        "  Synthetic : {} ({:.1}%)",
        total_synthetic,
        100.0 * total_synthetic as f64 / total
    );

    if let Some(sol_chunk) = manifest.chunk_by_id(&stellar_types::ChunkId(0, 0, 0)) {
        println!("  Sol chunk (0,0,0) star count   : {}", sol_chunk.star_count);
        println!("  Sol chunk (0,0,0) planet-hosts : {}", sol_chunk.planet_star_count);
    } else {
        println!("  Sol chunk (0,0,0): not found in manifest");
    }
    println!("────────────────────────────────────────────────");
}

fn progress_bar(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg} [{elapsed_precise}]")
            .unwrap(),
    );
    pb.set_message(msg.to_string());
    pb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_star_record_smoke() {
        let star = StarRecord {
            gaia_source_id: 1234567890,
            x: 0.0, y: 0.0, z: 0.0,
            magnitude: 4.83,
            color_index: 0.65,
            has_planets: false,
            quality: DataQuality::Inferred,
            planets: vec![],
        };
        assert_eq!(star.gaia_source_id, 1234567890);
    }

    #[test]
    fn test_convert_no_planets_solar_fallback() {
        let raw = RawStarRecord {
            source_id: 42,
            ra: 0.0, dec: 0.0, parallax: 100.0, parallax_error: 0.1,
            magnitude: 5.0,
            bp_rp: None,
            x: Some(10.0), y: Some(0.0), z: Some(0.0),
        };
        let stars = convert_to_star_records_no_planets(vec![raw]);
        assert_eq!(stars[0].color_index, 0.82);
        assert_eq!(stars[0].quality, DataQuality::Inferred);
        assert!(!stars[0].has_planets);
    }

    #[test]
    fn test_convert_no_planets_observed_when_bp_rp_present() {
        let raw = RawStarRecord {
            source_id: 1,
            ra: 0.0, dec: 0.0, parallax: 100.0, parallax_error: 0.1,
            magnitude: 5.0,
            bp_rp: Some(0.65),
            x: Some(1.0), y: Some(0.0), z: Some(0.0),
        };
        let stars = convert_to_star_records_no_planets(vec![raw]);
        assert_eq!(stars[0].color_index, 0.65);
        assert_eq!(stars[0].quality, DataQuality::Observed);
    }
}