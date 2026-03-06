# Stellar Voyage

A Rust/Bevy space exploration simulator built on real astronomical data from the ESA Gaia DR3 
catalog and NASA Exoplanet Archive.

## Workspace Structure
```
stellar/
├── stellar_types/        # Shared data types (StarRecord, PlanetRecord, DataQuality, etc.)
├── stellar_preprocessor/ # Offline CLI pipeline — ingests raw data, outputs binary chunks
├── stellar_voyage/       # Bevy game binary
├── assets/catalog/       # Preprocessor output — binary chunk files (not committed to git)
└── docs/                 # Architecture and design documents
```

## Crate Responsibilities

**`stellar_types`** — shared foundation. Defines all core structs and enums used by both the 
preprocessor and the game. Neither other crate crosses this boundary. Contains rkyv-derived 
types for zero-copy deserialization at runtime.

**`stellar_preprocessor`** — offline CLI tool. Ingests raw Gaia CSV/FITS and NASA Exoplanet 
Archive CSV, runs the full pipeline, and writes binary chunk files to `assets/catalog/`. 
Never shipped to end users.

**`stellar_voyage`** — the game. Loads pre-built binary chunks from `assets/catalog/` at 
runtime via Bevy's asset system. No database, no query planner, no SQL at runtime.

## Getting the Source Data

### ESA Gaia DR3

1. Go to the Gaia Archive: https://gea.esac.esa.int/archive
2. Use the ADQL query interface with this query for a development-sized extract (~500 pc):
```sql
SELECT source_id, ra, dec, parallax, parallax_error, phot_g_mean_mag, bp_rp
FROM gaiadr3.gaia_source
WHERE parallax > 2.0
AND parallax_over_error > 5
```

3. Download as CSV and place in a local directory (e.g. `data/gaia_source/`)
4. Do not commit raw source data — it is excluded by `.gitignore`

### NASA Exoplanet Archive

1. Go to: https://exoplanetarchive.ipac.caltech.edu
2. Select the **Planetary Systems (PS)** table — do not use the deprecated CPD table
3. Download as CSV with at minimum these columns:
   `pl_name, hostname, ra, dec, pl_orbsmax, pl_orbper, pl_rade, pl_eqt, discoverymethod`
4. Place the file at e.g. `data/exoplanet_source/PS_table.csv`

## Running the Preprocessor
```bash
cargo run -p stellar_preprocessor --release -- \
  --gaia-path data/gaia_source/ \
  --exoplanet-path data/exoplanet_source/PS_table.csv \
  --output-path assets/catalog/ \
  --chunk-size 50.0 \
  --stats
```

Optional flags:
- `--chunk-size 50.0` — parsec width of each spatial chunk (default 50)
- `--synthesise-gaps` — generate synthetic stars in underdense regions (default off)
- `--resume` — skip already-completed pipeline stages

Preprocessor output goes to `assets/catalog/` and is excluded from git.

## Verifying Chunk Output

After running the preprocessor, verify the binary chunk files with the standalone verifier:

```bash
cargo run -p stellar_preprocessor --bin verify_chunks --release -- \
  --catalog-path assets/catalog/
```

The verifier independently loads every chunk file via memory-mapped I/O, validates the rkyv
byte layout, confirms star counts match `catalog_manifest.json`, and spot-checks that all
star coordinates fall within their chunk's AABB. It must report zero errors before starting M5.

## Building the Game
```bash
cargo build -p stellar_voyage --release
cargo run -p stellar_voyage
```

The game requires preprocessed chunk files in `assets/catalog/` before it will load 
correctly. Run the preprocessor at least once against the development dataset first.

## Running Tests
```bash
# All workspace tests
cargo test --workspace

# Types crate only (rkyv round-trip tests)
cargo test -p stellar_types -- --nocapture

# Preprocessor unit + integration tests (includes Stage 5 chunk assignment and Stage 6 round-trip)
cargo test -p stellar_preprocessor -- --nocapture
```

## Development Milestones

| Milestone | Status |
|---|---|
| M1 — Types & Workspace | ✅ Complete |
| M2 — Preprocessor Ingest | ✅ Complete |
| M3 — Cross-Reference & Inference | ✅ Complete |
| M4 — Chunking & Serialisation | ✅ Complete |
| M5a — Game Foundation | ⬜ |
| M5b — Catalog Streaming | ⬜ |
| M6 — Navigation & Warp | ⬜ |
| M7 — Planetary Systems | ⬜ |
| M8 — HUD Complete | ⬜ |
| M9 — Polish | ⬜ |