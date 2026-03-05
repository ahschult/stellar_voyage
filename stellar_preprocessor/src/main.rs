// stellar_preprocessor/src/main.rs
//
// Offline CLI pipeline tool for Stellar Voyage.
// Ingests Gaia DR3 CSV and (M3+) NASA Exoplanet Archive data, runs all
// pipeline stages, and writes binary chunk assets to --output-path.
//
// Current stages (M2):
//   Stage 1 — Gaia CSV ingest + quality filters
//   Stage 2 — ICRS → Cartesian coordinate conversion
//
// Stages 3–6 are implemented in M3 and M4.

mod checkpoint;
mod error;
mod pipeline;
mod types;

use anyhow::Result;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

// ── CLI arguments ────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "stellar_preprocessor",
    about = "Gaia/Exoplanet preprocessing pipeline for Stellar Voyage"
)]
pub struct Args {
    /// Path to Gaia DR3 CSV file or directory of CSV files.
    #[arg(long)]
    pub gaia_path: std::path::PathBuf,

    /// Path to NASA Exoplanet Archive PS table CSV. Not required until M3.
    #[arg(long)]
    pub exoplanet_path: Option<std::path::PathBuf>,

    /// Output directory for chunk assets and pipeline artifacts.
    #[arg(long)]
    pub output_path: std::path::PathBuf,

    /// Parsec width of each spatial chunk cell (Stage 5, M4).
    #[arg(long, default_value_t = 50.0)]
    pub chunk_size: f64,

    /// Minimum parallax signal-to-noise ratio. Stars below this are excluded.
    /// Default 5.0 corresponds to ~20% distance uncertainty.
    #[arg(long, default_value_t = 5.0)]
    pub parallax_snr: f64,

    /// Angular separation threshold for coordinate proximity matching (Stage 3, M3).
    #[arg(long, default_value_t = 2.0)]
    pub proximity_arcsec: f64,

    /// Skip already-completed pipeline stages (uses .checkpoint sentinel files).
    #[arg(long)]
    pub resume: bool,

    /// Print pipeline summary statistics on completion.
    #[arg(long)]
    pub stats: bool,
}

// ── Entry point ──────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let args = Args::parse();

    std::fs::create_dir_all(&args.output_path)?;

    let mut error_log = error::ErrorLog::open(&args.output_path)?;

    // ── Stage 1: Gaia CSV ingest ──────────────────────────────────────────

    let stage1_checkpoint = args.output_path.join("stage1.bin");

    let mut raw_records = if args.resume
        && checkpoint::stage_complete(&args.output_path, "stage1")
    {
        println!("Stage 1: resuming from checkpoint…");
        load_stage1_checkpoint(&stage1_checkpoint)?
    } else {
        println!("Stage 1: ingesting Gaia source files…");
        let pb = spinner("  Ingesting");

        let result =
            pipeline::stage1::run(&args.gaia_path, args.parallax_snr, &mut error_log)?;

        pb.finish_with_message(format!(
            "  {} records passed ({} skipped, {} total rows)",
            result.records.len(),
            result.skipped_rows,
            result.total_rows,
        ));

        save_stage1_checkpoint(&stage1_checkpoint, &result.records)?;
        checkpoint::mark_complete(&args.output_path, "stage1")?;
        result.records
    };

    // ── Stage 2: Coordinate conversion ───────────────────────────────────

    if args.resume && checkpoint::stage_complete(&args.output_path, "stage2") {
        println!("Stage 2: resuming from checkpoint (coordinates already converted).");
        // The stage1.bin checkpoint already contains the converted coordinates
        // because we save after Stage 2 runs. Nothing to re-load.
    } else {
        println!("Stage 2: converting ICRS coordinates to parsec-space xyz…");
        let pb = spinner("  Converting");

        pipeline::stage2::run(&mut raw_records);

        pb.finish_with_message(format!(
            "  {} records converted",
            raw_records.len()
        ));

        // Overwrite stage1.bin with coordinates now populated.
        save_stage1_checkpoint(&stage1_checkpoint, &raw_records)?;
        checkpoint::mark_complete(&args.output_path, "stage2")?;
    }

    // ── Stages 3–4 (M3) placeholder ──────────────────────────────────────

    if let Some(ref _exo_path) = args.exoplanet_path {
        println!("--exoplanet-path provided but Stage 3/4 not yet implemented (M3).");
    } else {
        println!("(No --exoplanet-path provided; skipping stages 3 and 4.)");
    }

    // ── Stats ─────────────────────────────────────────────────────────────

    if args.stats {
        let with_color = raw_records.iter().filter(|r| r.bp_rp.is_some()).count();
        let total = raw_records.len();

        println!();
        println!("── Pipeline Stats (M2) ─────────────────────────────");
        println!(
            "  Total passing records : {}",
            total
        );
        println!(
            "  Records with bp_rp    : {} ({:.1}%)",
            with_color,
            if total > 0 {
                100.0 * with_color as f64 / total as f64
            } else {
                0.0
            }
        );
        println!("  Error log entries     : {}", error_log.count);
        println!(
            "  Output directory      : {}",
            args.output_path.display()
        );
    }

    error_log.flush();
    Ok(())
}

// ── Checkpoint serialisation ─────────────────────────────────────────────────
//
// The intermediate format uses bincode — fast binary serialisation that is
// entirely separate from rkyv. rkyv is reserved for the final StarRecord
// chunk files written in Stage 6 (M4). Mixing the two would risk version
// coupling in the intermediate artifacts.

fn save_stage1_checkpoint(
    path: &std::path::Path,
    records: &[types::RawStarRecord],
) -> Result<()> {
    let bytes = bincode::serialize(records)
        .map_err(|e| anyhow::anyhow!("bincode serialize failed: {}", e))?;
    checkpoint::write_atomic(path, &bytes)?;
    Ok(())
}

fn load_stage1_checkpoint(
    path: &std::path::Path,
) -> Result<Vec<types::RawStarRecord>> {
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("Failed to read stage1 checkpoint {:?}: {}", path, e))?;
    let records: Vec<types::RawStarRecord> = bincode::deserialize(&bytes)
        .map_err(|e| anyhow::anyhow!("bincode deserialize failed: {}", e))?;
    Ok(records)
}

// ── Progress bar helper ───────────────────────────────────────────────────────

fn spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg} [{elapsed_precise}]")
            .unwrap(),
    );
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

// ── Smoke tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use stellar_types::{DataQuality, StarRecord};

    #[test]
    fn smoke_star_record_import() {
        let star = StarRecord {
            gaia_source_id: 1_234_567_890,
            x: 0.0,
            y: 0.0,
            z: 0.0,
            magnitude: 4.83,
            color_index: 0.65,
            has_planets: false,
            quality: DataQuality::Observed,
            planets: vec![],
        };
        assert_eq!(star.gaia_source_id, 1_234_567_890);
    }
}