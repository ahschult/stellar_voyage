// stellar_preprocessor/tests/stage6_roundtrip.rs
//
// Integration test: serialise a minimal chunk with stage6's rkyv format,
// write it to a tempdir, reload it, and verify all fields survive intact.
//
// Run: cargo test -p stellar_preprocessor

use stellar_types::{ChunkId, DataQuality, PlanetRecord, StarRecord};
use tempfile::TempDir;

#[test]
fn stage6_chunk_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let output_dir = tmp.path();

    // A star with one planet — exercises the nested Vec<PlanetRecord> path.
    let star = StarRecord {
        gaia_source_id: 42,
        x: 10.0,
        y: 5.0,
        z: -3.0,
        magnitude: 5.5,
        color_index: 0.82,
        has_planets: true,
        quality: DataQuality::Observed,
        planets: vec![PlanetRecord {
            planet_name: "b".to_string(),
            semi_major_au: 1.0,
            period_days: 365.0,
            radius_earth: Some(1.0),
            eq_temp_kelvin: Some(255.0),
            quality: DataQuality::Observed,
        }],
    };

    // Serialise exactly as Stage 6 does.
    let bytes = rkyv::to_bytes::<_, 4096>(&vec![star.clone()]).unwrap();
    let bin_path = output_dir.join("catalog_0_0_0.bin");
    std::fs::write(&bin_path, bytes.as_slice()).unwrap();

    // Reload and validate — mirrors what verify_chunks and the game do.
    let data = std::fs::read(&bin_path).unwrap();
    let archived = rkyv::check_archived_root::<Vec<StarRecord>>(&data).unwrap();

    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].gaia_source_id, 42);
    assert_eq!(archived[0].x as f64, 10.0);
    assert_eq!(archived[0].y as f64, 5.0);
    assert_eq!(archived[0].z as f64, -3.0);
    assert!(archived[0].has_planets);
    assert_eq!(archived[0].planets.len(), 1);
    assert_eq!(archived[0].planets[0].planet_name.as_str(), "b");
    assert_eq!(archived[0].planets[0].radius_earth, Some(1.0_f32));
    assert_eq!(archived[0].planets[0].eq_temp_kelvin, Some(255.0_f32));
}

#[test]
fn stage6_chunk_roundtrip_multiple_stars() {
    let tmp = TempDir::new().unwrap();
    let output_dir = tmp.path();

    let make_star = |id: u64, x: f64| StarRecord {
        gaia_source_id: id,
        x,
        y: 0.0,
        z: 0.0,
        magnitude: 5.0,
        color_index: 0.8,
        has_planets: false,
        quality: DataQuality::Observed,
        planets: vec![],
    };

    let stars = vec![make_star(1, 10.0), make_star(2, 20.0), make_star(3, 30.0)];
    let bytes = rkyv::to_bytes::<_, 4096>(&stars).unwrap();
    let bin_path = output_dir.join("catalog_0_0_0.bin");
    std::fs::write(&bin_path, bytes.as_slice()).unwrap();

    let data = std::fs::read(&bin_path).unwrap();
    let archived = rkyv::check_archived_root::<Vec<StarRecord>>(&data).unwrap();

    assert_eq!(archived.len(), 3);
    assert_eq!(archived[0].gaia_source_id, 1);
    assert_eq!(archived[2].gaia_source_id, 3);
    assert_eq!(archived[2].x as f64, 30.0);
}

#[test]
fn stage6_chunk_roundtrip_negative_coordinates() {
    // Negative-coordinate stars (common near Sol) must survive the round-trip.
    let tmp = TempDir::new().unwrap();
    let output_dir = tmp.path();

    let star = StarRecord {
        gaia_source_id: 99,
        x: -25.3,
        y: -0.7,
        z: -12.1,
        magnitude: 8.2,
        color_index: 1.1,
        has_planets: false,
        quality: DataQuality::Inferred,
        planets: vec![],
    };

    let bytes = rkyv::to_bytes::<_, 4096>(&vec![star]).unwrap();
    let bin_path = output_dir.join("catalog_-1_-1_-1.bin");
    std::fs::write(&bin_path, bytes.as_slice()).unwrap();

    let data = std::fs::read(&bin_path).unwrap();
    let archived = rkyv::check_archived_root::<Vec<StarRecord>>(&data).unwrap();

    assert_eq!(archived.len(), 1);
    assert!((archived[0].x as f64 - (-25.3_f64)).abs() < 1e-9);
    assert!((archived[0].z as f64 - (-12.1_f64)).abs() < 1e-9);
    assert_eq!(archived[0].quality, stellar_types::DataQuality::Inferred);
}

#[test]
fn stage6_chunk_roundtrip_empty_chunk() {
    // Stage 6 should never write an empty chunk, but the format must support it.
    let tmp = TempDir::new().unwrap();
    let output_dir = tmp.path();

    let stars: Vec<StarRecord> = vec![];
    let bytes = rkyv::to_bytes::<_, 4096>(&stars).unwrap();
    let bin_path = output_dir.join("catalog_99_99_99.bin");
    std::fs::write(&bin_path, bytes.as_slice()).unwrap();

    let data = std::fs::read(&bin_path).unwrap();
    let archived = rkyv::check_archived_root::<Vec<StarRecord>>(&data).unwrap();

    assert_eq!(archived.len(), 0);
}