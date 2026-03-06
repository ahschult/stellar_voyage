// stellar_preprocessor/src/bin/verify_chunks.rs
// Run: cargo run -p stellar_preprocessor --bin verify_chunks --release -- \
//        --catalog-path assets/catalog/

use std::path::PathBuf;
use memmap2::MmapOptions;
use stellar_types::{StarRecord, CatalogManifest};
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    catalog_path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load manifest.
    let manifest_path = args.catalog_path.join("catalog_manifest.json");
    let manifest_json = std::fs::read_to_string(&manifest_path)?;
    let manifest: CatalogManifest = serde_json::from_str(&manifest_json)?;

    println!(
        "Manifest loaded: {} chunks, {} total stars",
        manifest.chunks.len(),
        manifest.total_stars
    );

    let mut total_stars_loaded: u64 = 0;
    let mut errors: Vec<String> = Vec::new();

    for chunk_meta in &manifest.chunks {
        let bin_path = args.catalog_path.join(&chunk_meta.file_name);

        let file = std::fs::File::open(&bin_path)
            .map_err(|e| anyhow::anyhow!("Cannot open {}: {}", chunk_meta.file_name, e))?;

        // SAFETY: the backing file is a read-only catalog written by Stage 6 and
        // is never modified or truncated at runtime, satisfying memmap2's requirements.
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        // check_archived_root validates the rkyv byte layout before any access.
        let archived = rkyv::check_archived_root::<Vec<StarRecord>>(&mmap)
            .map_err(|e| anyhow::anyhow!(
                "rkyv validation failed for {}: {:?}", chunk_meta.file_name, e
            ))?;

        let star_count = archived.len();
        total_stars_loaded += star_count as u64;

        // Verify star count matches the manifest entry.
        if star_count != chunk_meta.star_count as usize {
            errors.push(format!(
                "Chunk {}: manifest says {} stars, file contains {}",
                chunk_meta.id, chunk_meta.star_count, star_count
            ));
        }

        // Spot-check: every star's xyz must lie within the chunk's AABB.
        // A 0.001 pc tolerance absorbs f64 boundary rounding.
        let tol = 0.001_f64;
        for star in archived.iter() {
            let x = star.x as f64;
            let y = star.y as f64;
            let z = star.z as f64;
            if x < chunk_meta.aabb_min[0] - tol
                || x > chunk_meta.aabb_max[0] + tol
                || y < chunk_meta.aabb_min[1] - tol
                || y > chunk_meta.aabb_max[1] + tol
                || z < chunk_meta.aabb_min[2] - tol
                || z > chunk_meta.aabb_max[2] + tol
            {
                errors.push(format!(
                    "Chunk {}: star {} outside AABB: ({:.3}, {:.3}, {:.3})",
                    chunk_meta.id, star.gaia_source_id, x, y, z
                ));
                break; // one error per chunk is enough
            }
        }
    }

    // Report all errors before exiting.
    if !errors.is_empty() {
        eprintln!("VERIFICATION FAILED:");
        for e in &errors {
            eprintln!("  {}", e);
        }
        std::process::exit(1);
    }

    // Verify aggregate star count matches manifest total.
    if total_stars_loaded != manifest.total_stars {
        eprintln!(
            "TOTAL MISMATCH: manifest says {}, loaded {}",
            manifest.total_stars, total_stars_loaded
        );
        std::process::exit(1);
    }

    println!(
        "✓ All {} chunks verified. {} total stars loaded correctly.",
        manifest.chunks.len(),
        total_stars_loaded
    );

    // Spot-check: find the Sol chunk (0,0,0) and print its first 3 stars.
    if let Some(sol_meta) = manifest.chunk_by_id(&stellar_types::ChunkId(0, 0, 0)) {
        let bin_path = args.catalog_path.join(&sol_meta.file_name);
        let file = std::fs::File::open(&bin_path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let archived = rkyv::check_archived_root::<Vec<StarRecord>>(&mmap)
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        println!("\nSol chunk (0,0,0) — first 3 stars:");
        for star in archived.iter().take(3) {
            println!(
                "  gaia_id={} xyz=({:.3}, {:.3}, {:.3}) mag={:.2} bp_rp={:.2} planets={}",
                star.gaia_source_id,
                star.x, star.y, star.z,
                star.magnitude, star.color_index,
                star.planets.len()
            );
        }
    } else {
        println!("\nNote: Sol chunk (0,0,0) not present in this catalog.");
    }

    Ok(())
}