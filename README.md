# Stellar Voyage

A multi-stage Rust data pipeline ingesting ESA Gaia DR3 (~1.8B stars) and NASA Exoplanet Archive into a zero-copy binary catalog, with a Bevy-based space exploration front end built on the processed data.

The primary engineering work in this repository is the preprocessor pipeline — a production-grade ETL system handling astronomical data at scale, with spatial chunking, multi-source cross-referencing, field-level data provenance classification, and memory-mapped binary serialization designed for sub-millisecond runtime asset loads. The game is the demonstration layer on top of that infrastructure.

> **Note on commit history:** This project was developed in a private Gitea repository from 2026 onward and migrated here for public visibility. The full incremental commit history has been preserved on migration. Active development continues.

---

## Workspace Structure

```
stellar/
├── stellar_types/        # Shared data types (StarRecord, PlanetRecord, DataQuality, etc.)
├── stellar_preprocessor/ # Offline CLI pipeline — ingests raw data, outputs binary chunks
├── stellar_voyage/       # Bevy game binary
├── assets/catalog/       # Preprocessor output — binary chunk files (not committed to git)
└── docs/                 # Architecture and design documents
```

---

## Pipeline Architecture

The preprocessor runs as an offline CLI tool across six discrete stages. Each stage is independently resumable via `--resume`. Raw source data never touches the game binary.

### Stage Overview

| Stage | Description |
|-------|-------------|
| 1 — Source Filtering | Ingest raw Gaia CSV/FITS and NASA Exoplanet Archive CSV; apply parallax quality gates and signal-to-noise thresholds |
| 2 — Coordinate Transformation | Convert raw equatorial coordinates (RA/Dec/parallax) to Cartesian parsec-space (XYZ) |
| 3 — Multi-Source Cross-Referencing | Join Gaia stellar records against exoplanet host stars by coordinate proximity; annotate matched records |
| 4 — Provenance Classification | Tag every field by observation quality: **Measured** (direct observation), **Inferred** (derived from correlated signals), or **Synthesized** (procedurally generated to fill gaps) |
| 5 — Spatial Chunking | Assign stars to configurable spatial chunks (default 50-parsec cells); build chunk manifest with AABB metadata |
| 6 — Binary Serialization | Serialize each chunk to rkyv binary format with zero-copy deserialization layout; write catalog manifest |

### Data Provenance — "Data Honesty" Framework

Every field in `StarRecord` and `PlanetRecord` carries a `DataQuality` tag:

- **`Measured`** — directly observed value from the source catalog (e.g. Gaia parallax, photometry)
- **`Inferred`** — derived from correlated signals using known physical relationships (e.g. distance from parallax inversion, luminosity from magnitude + distance)
- **`Synthesized`** — procedurally generated to fill spatial gaps when `--synthesise-gaps` is enabled

These tags are surfaced live in the game UI so the player always knows the epistemic status of what they are looking at. The framework is designed to be the authoritative source of data confidence throughout the pipeline — nothing is presented as measured that was not measured.

### Zero-Copy Runtime Loading

The game loads binary chunks via memory-mapped I/O using [rkyv](https://github.com/rkyv/rkyv). No database, no query planner, no SQL at runtime. A chunk is mapped into memory and cast directly to the target type — the deserialization cost is effectively zero. On typical hardware this produces sub-millisecond asset loads for chunks containing tens of thousands of stars.

---

## Getting the Source Data

### ESA Gaia DR3

1. Go to the Gaia Archive: https://gea.esac.esa.int/archive
2. Use the ADQL query interface. For a development-sized extract (~500 pc):

```sql
SELECT source_id, ra, dec, parallax, parallax_error, phot_g_mean_mag, bp_rp
FROM gaiadr3.gaia_source
WHERE parallax > 2.0
AND parallax_over_error > 5
```

3. Download as CSV and place in a local directory (e.g. `data/gaia_source/`)
4. Raw source data is excluded by `.gitignore` — do not commit it

For the full DR3 catalog (~1.8B rows), use the bulk download tools documented at the Gaia Archive. Pipeline memory usage scales with chunk size, not total catalog size.

### NASA Exoplanet Archive

1. Go to: https://exoplanetarchive.ipac.caltech.edu
2. Select the **Planetary Systems (PS)** table — do not use the deprecated CPD table
3. Download as CSV with at minimum these columns:
   `pl_name, hostname, ra, dec, pl_orbsmax, pl_orbper, pl_rade, pl_eqt, discoverymethod`
4. Place the file at e.g. `data/exoplanet_source/PS_table.csv`

---

## Running the Preprocessor

```bash
cargo run -p stellar_preprocessor --release -- \
  --gaia-path data/gaia_source/ \
  --exoplanet-path data/exoplanet_source/PS_table.csv \
  --output-path assets/catalog/ \
  --chunk-size 50.0 \
  --stats
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--chunk-size 50.0` | Parsec width of each spatial chunk (default 50) |
| `--synthesise-gaps` | Generate synthetic stars in underdense regions (default off) |
| `--resume` | Skip already-completed pipeline stages |
| `--stats` | Print per-stage record counts, timing, and provenance distribution |

Preprocessor output goes to `assets/catalog/` and is excluded from git.

---

## Verifying Chunk Output

After running the preprocessor, verify the binary chunk files before building the game:

```bash
cargo run -p stellar_preprocessor --bin verify_chunks --release -- \
  --catalog-path assets/catalog/
```

The verifier independently loads every chunk file via memory-mapped I/O, validates the rkyv byte layout, confirms star counts match `catalog_manifest.json`, and spot-checks that all star coordinates fall within their chunk's AABB. It must report zero errors before proceeding to game builds.

---

## Building and Running the Game

```bash
cargo build -p stellar_voyage --release
cargo run -p stellar_voyage
```

The game requires preprocessed chunk files in `assets/catalog/` before it will load correctly. Run the preprocessor at least once against the development dataset first.

---

## Running Tests

```bash
# All workspace tests
cargo test --workspace

# stellar_types only (rkyv round-trip tests, DataQuality tag preservation)
cargo test -p stellar_types -- --nocapture

# Preprocessor unit + integration tests
# Includes Stage 5 chunk assignment, Stage 6 binary round-trip, and provenance tag propagation
cargo test -p stellar_preprocessor -- --nocapture
```

---

## Crate Responsibilities

**`stellar_types`** — shared foundation. Defines all core structs and enums used across the workspace, including `StarRecord`, `PlanetRecord`, and `DataQuality`. Neither downstream crate crosses this boundary. All types carry rkyv-derived implementations for zero-copy deserialization.

**`stellar_preprocessor`** — the pipeline. Offline CLI tool handling all six ingestion stages. Never shipped to end users. Contains the ADQL query tooling, coordinate transformation math, cross-referencing logic, provenance classification engine, spatial chunker, and binary serializer.

**`stellar_voyage`** — the game. Loads pre-built binary chunks from `assets/catalog/` at runtime via Bevy's asset system. No database, no query planner, no SQL at runtime. Surfaces DataQuality provenance tags live in the UI.
