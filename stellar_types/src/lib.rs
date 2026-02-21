// stellar_types/src/lib.rs
use serde::{Deserialize, Serialize};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub enum DataQuality {
    Observed,   // Value from direct catalog measurement
    Inferred,   // Value estimated from astrophysical relations
    Synthetic,  // Record generated to fill spatial catalog gap
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct PlanetRecord {
    pub planet_name: String,
    pub semi_major_au: f32,
    pub period_days: f32,
    pub radius_earth: Option<f32>,
    pub eq_temp_kelvin: Option<f32>,
    pub quality: DataQuality,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct StarRecord {
    pub gaia_source_id: u64,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub magnitude: f32,
    pub color_index: f32,
    pub has_planets: bool,
    pub quality: DataQuality,
    pub planets: Vec<PlanetRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub struct ChunkId(pub i32, pub i32, pub i32);

impl std::fmt::Display for ChunkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}_{}", self.0, self.1, self.2)
    }
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct ChunkMeta {
    pub id: ChunkId,
    pub star_count: u32,
    pub planet_star_count: u32,
    pub observed_count: u32,
    pub inferred_count: u32,
    pub synthetic_count: u32,
    pub aabb_min: [f64; 3],
    pub aabb_max: [f64; 3],
    pub file_name: String,
}

#[derive(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct CatalogManifest {
    pub version: u32,
    pub chunk_size_parsecs: f64,
    pub total_stars: u64,
    pub total_planets: u32,
    pub chunks: Vec<ChunkMeta>,
}

impl CatalogManifest {
    pub fn chunk_by_id(&self, id: &ChunkId) -> Option<&ChunkMeta> {
        self.chunks.iter().find(|chunk| &chunk.id == id)
    }

    pub fn chunks_in_radius(
        &self,
        center: [f64; 3],
        radius: f64,
    ) -> impl Iterator<Item = &ChunkMeta> {
        self.chunks.iter().filter(move |chunk| {
            let dx = (chunk.aabb_min[0].max(center[0] - radius)
                - chunk.aabb_max[0].min(center[0] + radius))
                .max(0.0);
            let dy = (chunk.aabb_min[1].max(center[1] - radius)
                - chunk.aabb_max[1].min(center[1] + radius))
                .max(0.0);
            let dz = (chunk.aabb_min[2].max(center[2] - radius)
                - chunk.aabb_max[2].min(center[2] + radius))
                .max(0.0);

            dx == 0.0 && dy == 0.0 && dz == 0.0
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::Deserialize as RkyvDeserialize;

    // --- Helpers ---

    fn make_planet(name: &str, with_data: bool) -> PlanetRecord {
        PlanetRecord {
            planet_name: name.to_string(),
            semi_major_au: 1.0,
            period_days: 365.25,
            radius_earth: if with_data { Some(1.0) } else { None },
            eq_temp_kelvin: if with_data { Some(288.0) } else { None },
            quality: DataQuality::Observed,
        }
    }

    fn rkyv_roundtrip_star(star: &StarRecord) -> StarRecord {
        let bytes = rkyv::to_bytes::<_, 4096>(star).expect("serialization failed");
        let archived = unsafe { rkyv::archived_root::<StarRecord>(&bytes) };
        archived
            .deserialize(&mut rkyv::Infallible)
            .expect("deserialization failed")
    }

    fn rkyv_roundtrip_planet(planet: &PlanetRecord) -> PlanetRecord {
        let bytes = rkyv::to_bytes::<_, 256>(planet).expect("serialization failed");
        let archived = unsafe { rkyv::archived_root::<PlanetRecord>(&bytes) };
        archived
            .deserialize(&mut rkyv::Infallible)
            .expect("deserialization failed")
    }

    // --- PlanetRecord basic round-trips ---

    #[test]
    fn planet_roundtrip_all_some() {
        let planet = make_planet("Kepler-442b", true);
        let rt = rkyv_roundtrip_planet(&planet);
        assert_eq!(rt.planet_name, planet.planet_name);
        assert_eq!(rt.radius_earth, Some(1.0));
        assert_eq!(rt.eq_temp_kelvin, Some(288.0));
        assert_eq!(rt.quality, DataQuality::Observed);
    }

    #[test]
    fn planet_roundtrip_all_none() {
        let planet = make_planet("HD-209458b", false);
        let rt = rkyv_roundtrip_planet(&planet);
        assert_eq!(rt.planet_name, planet.planet_name);
        assert_eq!(rt.radius_earth, None);
        assert_eq!(rt.eq_temp_kelvin, None);
    }

    // --- StarRecord basic round-trips ---

    #[test]
    fn star_roundtrip_no_planets() {
        let star = StarRecord {
            gaia_source_id: 12345,
            x: 1.34,
            y: 0.0,
            z: 0.0,
            magnitude: 4.38,
            color_index: 0.71,
            has_planets: false,
            quality: DataQuality::Observed,
            planets: vec![],
        };
        let rt = rkyv_roundtrip_star(&star);
        assert_eq!(rt.gaia_source_id, 12345);
        assert_eq!(rt.x, 1.34);
        assert_eq!(rt.planets.len(), 0);
        assert_eq!(rt.quality, DataQuality::Observed);
    }

    #[test]
    fn star_roundtrip_two_planets() {
        let star = StarRecord {
            gaia_source_id: 99999,
            x: 7.7,
            y: 0.0,
            z: 0.0,
            magnitude: 0.03,
            color_index: 0.0,
            has_planets: true,
            quality: DataQuality::Observed,
            planets: vec![
                make_planet("Vega-b", true),
                make_planet("Vega-c", false),
            ],
        };
        let rt = rkyv_roundtrip_star(&star);
        assert_eq!(rt.planets.len(), 2);
        assert_eq!(rt.planets[0].planet_name, "Vega-b");
        assert_eq!(rt.planets[1].radius_earth, None);
    }

    // --- Edge cases ---

    #[test]
    fn star_roundtrip_many_planets() {
        let planets: Vec<PlanetRecord> = (0..25)
            .map(|i| make_planet(&format!("Planet-{}", i), i % 2 == 0))
            .collect();

        let star = StarRecord {
            gaia_source_id: 1,
            x: 0.0, y: 0.0, z: 0.0,
            magnitude: 5.0,
            color_index: 0.5,
            has_planets: true,
            quality: DataQuality::Inferred,
            planets,
        };
        let rt = rkyv_roundtrip_star(&star);
        assert_eq!(rt.planets.len(), 25);
        assert_eq!(rt.planets[24].planet_name, "Planet-24");
    }

    #[test]
    fn planet_roundtrip_unicode_name() {
        let planet = make_planet("Próxima Centauri b", true);
        let rt = rkyv_roundtrip_planet(&planet);
        assert_eq!(rt.planet_name, "Próxima Centauri b");
    }

    #[test]
    fn star_roundtrip_extreme_coordinates() {
        let cases = [
            (1.34, 0.0, 0.0),          // Sol neighbourhood — Alpha Centauri
            (-8178.0, 0.0, 17.0),      // Galactic centre
            (50_000.0, 0.0, 0.0),      // Large Magellanic Cloud distance
        ];
        for (x, y, z) in cases {
            let star = StarRecord {
                gaia_source_id: 1,
                x, y, z,
                magnitude: 5.0,
                color_index: 0.5,
                has_planets: false,
                quality: DataQuality::Observed,
                planets: vec![],
            };
            let rt = rkyv_roundtrip_star(&star);
            assert_eq!(rt.x, x, "x mismatch at ({x}, {y}, {z})");
            assert_eq!(rt.y, y);
            assert_eq!(rt.z, z);
        }
    }

    #[test]
    fn data_quality_all_variants_roundtrip() {
        for quality in [DataQuality::Observed, DataQuality::Inferred, DataQuality::Synthetic] {
            let bytes = rkyv::to_bytes::<_, 64>(&quality).expect("serialize failed");
            let archived = unsafe { rkyv::archived_root::<DataQuality>(&bytes) };
            let rt: DataQuality = archived.deserialize(&mut rkyv::Infallible).unwrap();
            assert_eq!(rt, quality);
        }
    }

    #[test]
    fn star_roundtrip_synthetic_no_planets() {
        let star = StarRecord {
            gaia_source_id: 0,
            x: 1000.0, y: 200.0, z: -50.0,
            magnitude: 12.0,
            color_index: 1.5,
            has_planets: false,
            quality: DataQuality::Synthetic,
            planets: vec![],
        };
        let rt = rkyv_roundtrip_star(&star);
        assert_eq!(rt.quality, DataQuality::Synthetic);
        assert!(!rt.has_planets);
        assert_eq!(rt.planets.len(), 0);
    }

    // --- JSON round-trips ---

    #[test]
    fn chunk_id_display_format() {
        let id = ChunkId(3, -1, 7);
        assert_eq!(id.to_string(), "3_-1_7");

        let id_zero = ChunkId(0, 0, 0);
        assert_eq!(id_zero.to_string(), "0_0_0");
    }

    #[test]
    fn catalog_manifest_json_roundtrip() {
        let manifest = CatalogManifest {
            version: 1,
            chunk_size_parsecs: 50.0,
            total_stars: 1_000_000,
            total_planets: 5432,
            chunks: vec![
                ChunkMeta {
                    id: ChunkId(0, 0, 0),
                    star_count: 500,
                    planet_star_count: 12,
                    observed_count: 490,
                    inferred_count: 10,
                    synthetic_count: 0,
                    aabb_min: [0.0, 0.0, 0.0],
                    aabb_max: [50.0, 50.0, 50.0],
                    file_name: "catalog_0_0_0.bin".to_string(),
                },
                ChunkMeta {
                    id: ChunkId(1, 0, 0),
                    star_count: 300,
                    planet_star_count: 5,
                    observed_count: 200,
                    inferred_count: 80,
                    synthetic_count: 20,
                    aabb_min: [50.0, 0.0, 0.0],
                    aabb_max: [100.0, 50.0, 50.0],
                    file_name: "catalog_1_0_0.bin".to_string(),
                },
                ChunkMeta {
                    id: ChunkId(-1, 0, 0),
                    star_count: 0,
                    planet_star_count: 0,
                    observed_count: 0,
                    inferred_count: 0,
                    synthetic_count: 150,
                    aabb_min: [-50.0, 0.0, 0.0],
                    aabb_max: [0.0, 50.0, 50.0],
                    file_name: "catalog_-1_0_0.bin".to_string(),
                },
            ],
        };

        let json = serde_json::to_string(&manifest).expect("serialize failed");
        let rt: CatalogManifest = serde_json::from_str(&json).expect("deserialize failed");

        assert_eq!(rt.version, 1);
        assert_eq!(rt.total_stars, 1_000_000);
        assert_eq!(rt.chunks.len(), 3);
        assert_eq!(rt.chunks[2].id, ChunkId(-1, 0, 0));
        assert_eq!(rt.chunks[2].synthetic_count, 150);
    }

    #[test]
    fn manifest_chunk_by_id() {
        // uses the same manifest as above, just verifying the helper method
        let manifest = CatalogManifest {
            version: 1,
            chunk_size_parsecs: 50.0,
            total_stars: 0,
            total_planets: 0,
            chunks: vec![
                ChunkMeta {
                    id: ChunkId(3, -1, 7),
                    star_count: 100,
                    planet_star_count: 0,
                    observed_count: 100,
                    inferred_count: 0,
                    synthetic_count: 0,
                    aabb_min: [150.0, -50.0, 350.0],
                    aabb_max: [200.0, 0.0, 400.0],
                    file_name: "catalog_3_-1_7.bin".to_string(),
                },
            ],
        };

        assert!(manifest.chunk_by_id(&ChunkId(3, -1, 7)).is_some());
        assert!(manifest.chunk_by_id(&ChunkId(0, 0, 0)).is_none());
    }
}