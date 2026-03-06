// stellar_preprocessor/src/pipeline/stage5.rs
//
// Stage 5: Chunk Assignment
//
// Takes the final Vec<StarRecord> from Stage 4 and assigns each star to a
// ChunkId by dividing its xyz position by chunk_size and flooring.
//
// This is O(n) with no I/O — fast, no parallelisation needed.

use stellar_types::{ChunkId, StarRecord};
use crate::types::ChunkedStar;

pub struct Stage5Result {
    pub chunked: Vec<ChunkedStar>,
    pub chunk_count: usize,
}

pub fn run(records: Vec<StarRecord>, chunk_size: f64) -> Stage5Result {
    let mut chunked: Vec<ChunkedStar> = records
        .into_iter()
        .map(|record| {
            // IMPORTANT: use .floor() before casting — NOT bare `as i32`.
            // Bare truncation rounds toward zero; floor rounds toward -∞.
            // Without this, stars at x ∈ (-chunk_size, 0) land in chunk 0
            // instead of chunk -1. Sol's neighbourhood has negative-coordinate
            // stars that must land in negative chunks.
            let ix = (record.x / chunk_size).floor() as i32;
            let iy = (record.y / chunk_size).floor() as i32;
            let iz = (record.z / chunk_size).floor() as i32;
            let chunk_id = ChunkId(ix, iy, iz);
            ChunkedStar { chunk_id, record }
        })
        .collect();

    // Sort by chunk ID for deterministic linear grouping in Stage 6.
    // All stars in the same chunk will be contiguous after this sort.
    chunked.sort_unstable_by_key(|cs| (cs.chunk_id.0, cs.chunk_id.1, cs.chunk_id.2));

    let chunk_count = {
        let mut ids: Vec<ChunkId> = chunked.iter().map(|cs| cs.chunk_id).collect();
        ids.dedup();
        ids.len()
    };

    Stage5Result { chunked, chunk_count }
}

// ── Unit Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_types::DataQuality;

    fn make_star(x: f64, y: f64, z: f64) -> StarRecord {
        StarRecord {
            gaia_source_id: 0,
            x,
            y,
            z,
            magnitude: 5.0,
            color_index: 0.8,
            has_planets: false,
            quality: DataQuality::Observed,
            planets: vec![],
        }
    }

    #[test]
    fn test_chunk_assignment_positive() {
        let result = run(vec![make_star(75.0, 0.0, 0.0)], 50.0);
        assert_eq!(result.chunked[0].chunk_id, ChunkId(1, 0, 0));
    }

    #[test]
    fn test_chunk_assignment_negative_floor() {
        // x = -25.0 → chunk -1, NOT chunk 0.
        // Bare `as i32` (truncation) would return 0. floor() returns -1.
        let result = run(vec![make_star(-25.0, 0.0, 0.0)], 50.0);
        assert_eq!(result.chunked[0].chunk_id, ChunkId(-1, 0, 0));
    }

    #[test]
    fn test_chunk_assignment_at_boundary() {
        // x = 50.0 is exactly on the boundary — goes to chunk 1, not 0.
        let result = run(vec![make_star(50.0, 0.0, 0.0)], 50.0);
        assert_eq!(result.chunked[0].chunk_id, ChunkId(1, 0, 0));
    }

    #[test]
    fn test_chunk_assignment_sol() {
        // Sol at (0, 0, 0) → chunk (0, 0, 0).
        let result = run(vec![make_star(0.0, 0.0, 0.0)], 50.0);
        assert_eq!(result.chunked[0].chunk_id, ChunkId(0, 0, 0));
    }

    #[test]
    fn test_chunk_count_two_chunks() {
        let stars = vec![
            make_star(10.0, 0.0, 0.0),  // chunk (0,0,0)
            make_star(60.0, 0.0, 0.0),  // chunk (1,0,0)
            make_star(20.0, 0.0, 0.0),  // chunk (0,0,0)
        ];
        let result = run(stars, 50.0);
        assert_eq!(result.chunk_count, 2);
    }

    #[test]
    fn test_sort_is_deterministic() {
        // Same input in different order should produce same sorted output.
        let stars_a = vec![make_star(60.0, 0.0, 0.0), make_star(10.0, 0.0, 0.0)];
        let stars_b = vec![make_star(10.0, 0.0, 0.0), make_star(60.0, 0.0, 0.0)];
        let result_a = run(stars_a, 50.0);
        let result_b = run(stars_b, 50.0);
        let ids_a: Vec<ChunkId> = result_a.chunked.iter().map(|cs| cs.chunk_id).collect();
        let ids_b: Vec<ChunkId> = result_b.chunked.iter().map(|cs| cs.chunk_id).collect();
        assert_eq!(ids_a, ids_b);
    }

    #[test]
    fn test_negative_z_near_sol() {
        // Stars with small negative z (common near Sol) must land in chunk -1,
        // not chunk 0. Regression guard for the floor-vs-truncation bug.
        let result = run(vec![make_star(0.0, 0.0, -1.0)], 50.0);
        assert_eq!(result.chunked[0].chunk_id, ChunkId(0, 0, -1));
    }
}