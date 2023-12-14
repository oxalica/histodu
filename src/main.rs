use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;

use bytesize::ByteSize;
use clap::Parser;
use miniserde::Serialize;

mod cli;

#[derive(Debug, Serialize)]
struct Output {
    count: u64,
    mean: f64,
    at_quantile: BTreeMap<u8, u64>,
    quantile_of: BTreeMap<u64, f64>,
}

impl fmt::Display for Output {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "count = {}", self.count)?;
        let bytes = |b| ByteSize(b).to_string_as(true);
        writeln!(f, "mean = {}", bytes(self.mean as u64))?;
        for (&percent, &sz) in &self.at_quantile {
            writeln!(f, "{}% = {}", percent, ByteSize(sz))?;
        }
        for (&size, &q) in &self.quantile_of {
            writeln!(f, "{:.3}% = {}", q * 100.0, bytes(size))?;
        }
        Ok(())
    }
}

fn main() {
    let cli = cli::Cli::parse();

    let config = histodu::Config {
        one_file_system: cli.one_file_system,
        include_empty: cli.include_empty,
        threads: NonZeroUsize::new(cli.threads).unwrap_or_else(|| {
            std::thread::available_parallelism()
                .expect("failed to get available parallelism")
                .saturating_mul(NonZeroUsize::new(2).expect("2 is not zero"))
        }),
        on_error: &|path, err| eprintln!("{}: {}", path.display(), err),
    };

    let hist = match histodu::dir_size_histogram(&cli.root_path, &config) {
        Ok(hist) => hist,
        // Errors should already be reported via `on_error`.
        Err(()) => std::process::exit(1),
    };

    let out = Output {
        count: hist.len(),
        mean: hist.mean(),
        at_quantile: cli
            .at_quantile
            .iter()
            .map(|&percent| (percent, hist.value_at_quantile(percent as f64 / 100.0)))
            .collect(),
        quantile_of: cli
            .quantile_of
            .iter()
            .map(|&size| size.as_u64())
            .map(|bytes| (bytes, hist.quantile_below(bytes)))
            .collect(),
    };

    if cli.json {
        let out = miniserde::json::to_string(&out);
        println!("{out}");
    } else {
        println!("{out}");
    }
}
