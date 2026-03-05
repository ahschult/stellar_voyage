// stellar_preprocessor/src/error.rs

use anyhow::Result;
use std::io::Write;
use std::path::Path;

pub struct ErrorLog {
    writer: std::io::BufWriter<std::fs::File>,
    pub count: u64,
}

impl ErrorLog {
    pub fn open(output_dir: &Path) -> Result<Self> {
        let path = output_dir.join("pipeline_errors.log");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            writer: std::io::BufWriter::new(file),
            count: 0,
        })
    }

    /// Log a skipped record. Never panics — errors writing the log are suppressed.
    pub fn log_skip(&mut self, source_line: u64, field: &str, reason: &str) {
        self.count += 1;
        let _ = writeln!(
            self.writer,
            "SKIP line={} field={} reason={}",
            source_line, field, reason
        );
    }

    pub fn flush(&mut self) {
        let _ = self.writer.flush();
    }
}