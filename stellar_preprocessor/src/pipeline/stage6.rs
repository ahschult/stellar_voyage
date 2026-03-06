// stellar_preprocessor/src/pipeline/stage6.rs
//
// Stage 6: rkyv Chunk Serialisation
//
// Groups the sorted ChunkedStar vec by ChunkId, serialises each group to an
// rkyv AlignedVec, writes to a .bin file atomically via checkpoint::write_atomic,
// and accumulates metadata for catalog_manifest.json.
//
// The chunk file format is a raw rkyv archive of Vec<StarRecord>.
// At runtime the game accesses it as rkyv::Archived<Vec<StarRecord>> —
// zero-copy, no heap allocation of the full structs.

use std::path::Path;
use stellar_types::{ChunkId, ChunkMeta, CatalogManifest, StarRecord, DataQuality};
use crate::types::ChunkedStar;
use crate::checkpoint::write_atomic;
use anyhow::{Context, Result};

pub struct Stage6Result {
    pub manifest: CatalogManifest,
    pub files_written: usize,
}

pub fn run(
    chunked: Vec<ChunkedStar>,
    output_dir: &Path,
    chunk_size: f64,
    total_input_stars: u64,
) -> Result<Stage6Result> {
    // Group stars by chunk ID. The vec is sorted from Stage 5, so we can use
    // a linear group-by — no HashMap needed.
    let groups = group_by_chunk(chunked);
    let total_chunks = groups.len();
    let mut chunk_metas: Vec<ChunkMeta> = Vec::with_capacity(total_chunks);

    for (chunk_id, stars) in &groups {
        let file_name = format!("catalog_{}.bin", chunk_id);
        let file_path = output_dir.join(&file_name);

        // Serialise Vec<StarRecord> to rkyv bytes.
        // to_bytes returns an AlignedVec. The 4096-byte scratch parameter
        // reduces heap allocations for large chunks.
        let bytes = rkyv::to_bytes::<_, 4096>(stars)
            .with_context(|| format!("rkyv serialisation failed for chunk {}", chunk_id))?;

        write_atomic(&file_path, bytes.as_slice())?;

        let meta = compute_chunk_meta(chunk_id, stars, &file_name);
        chunk_metas.push(meta);
    }

    // Sort chunk_metas by ID for deterministic manifest output.
    chunk_metas.sort_by_key(|m| (m.id.0, m.id.1, m.id.2));

    // total_planets here counts planet-hosting stars (matching ChunkMeta semantics).
    let total_planets: u32 = chunk_metas.iter().map(|m| m.planet_star_count).sum();

    let manifest = CatalogManifest {
        version: 1,
        chunk_size_parsecs: chunk_size,
        total_stars: total_input_stars,
        total_planets,
        chunks: chunk_metas,
    };

    // Write manifest atomically.
    let manifest_path = output_dir.join("catalog_manifest.json");
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .context("Failed to serialise manifest to JSON")?;
    write_atomic(&manifest_path, manifest_json.as_bytes())?;

    Ok(Stage6Result {
        manifest,
        files_written: total_chunks,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Linear group-by — works because input is sorted by chunk_id from Stage 5.
fn group_by_chunk(chunked: Vec<ChunkedStar>) -> Vec<(ChunkId, Vec<StarRecord>)> {
    let mut groups: Vec<(ChunkId, Vec<StarRecord>)> = Vec::new();
    for cs in chunked {
        if let Some(last) = groups.last_mut() {
            if last.0 == cs.chunk_id {
                last.1.push(cs.record);
                continue;
            }
        }
        groups.push((cs.chunk_id, vec![cs.record]));
    }
    groups
}

fn compute_chunk_meta(
    id: &ChunkId,
    stars: &[StarRecord],
    file_name: &str,
) -> ChunkMeta {
    let mut aabb_min = [f64::MAX; 3];
    let mut aabb_max = [f64::MIN; 3];
    let mut observed_count  = 0u32;
    let mut inferred_count  = 0u32;
    let mut synthetic_count = 0u32;
    let mut planet_star_count = 0u32;

    for star in stars {
        aabb_min[0] = aabb_min[0].min(star.x);
        aabb_min[1] = aabb_min[1].min(star.y);
        aabb_min[2] = aabb_min[2].min(star.z);
        aabb_max[0] = aabb_max[0].max(star.x);
        aabb_max[1] = aabb_max[1].max(star.y);
        aabb_max[2] = aabb_max[2].max(star.z);

        match star.quality {
            DataQuality::Observed  => observed_count  += 1,
            DataQuality::Inferred  => inferred_count  += 1,
            DataQuality::Synthetic => synthetic_count += 1,
        }

        if star.has_planets {
            planet_star_count += 1;
        }
    }

    ChunkMeta {
        id: *id,
        star_count: stars.len() as u32,
        planet_star_count,
        observed_count,
        inferred_count,
        synthetic_count,
        aabb_min,
        aabb_max,
        file_name: file_name.to_string(),
    }
}