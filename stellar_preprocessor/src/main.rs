// stellar_preprocessor/src/lib.rs

use stellar_types::{StarRecord, DataQuality};

fn main() {
    println!("Stellar Preprocessor");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_star_record_smoke() {
        let star = StarRecord {
            gaia_source_id: 1234567890,
            x: 0.0,
            y: 0.0,
            z: 0.0,
            magnitude: 4.83,
            color_index: 0.65,
            has_planets: false,
            quality: DataQuality::Inferred,
            planets: vec![],
        };
        
        assert_eq!(star.gaia_source_id, 1234567890);
    }
}
