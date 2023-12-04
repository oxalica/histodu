use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use bytesize::ByteSize;
use clap::Parser;
use miniserde::Serialize;

#[derive(Debug, clap::Parser)]
#[command(version, about)]
struct Cli {
    /// Include all zero-length files.
    #[arg(long)]
    include_empty: bool,

    /// Print values at specific quantiles, instead of default [0%, 50%, 90%, 99%, 100%].
    /// The value is given in integer percentage in range [0, 100].
    #[arg(
        long,
        short = 'q',
        default_values = ["0", "50", "90", "99", "100"],
        value_parser = clap::value_parser!(u8).range(0..=100)
    )]
    at_quantile: Vec<u8>,

    /// Print quantiles below specific values, instead of default [4K, 64K, 1M].
    /// The value is given in bytes.
    #[arg(long, short = 'r', default_values = ["4096", "65536", "131072"])]
    quantile_of: Vec<u64>,

    /// Print output in JSON format.
    #[arg(long)]
    json: bool,

    /// The root path to account.
    root: PathBuf,

    /// The maximum concurrency. If set to zero, the effective value is
    /// twice the number of logical CPUs.
    #[arg(long, default_value = "0")]
    threads: usize,
}

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
        writeln!(f, "mean = {}", ByteSize(self.mean as u64))?;
        for (&percent, &sz) in &self.at_quantile {
            writeln!(f, "{}% = {}", percent, ByteSize(sz))?;
        }
        for (&size, &q) in &self.quantile_of {
            writeln!(f, "{} = {}%", ByteSize(size), q * 100.0)?;
        }
        Ok(())
    }
}

fn main() {
    let cli = Cli::parse();

    let config = filestat::Config {
        include_empty: cli.include_empty,
        threads: NonZeroUsize::new(cli.threads).unwrap_or_else(|| {
            std::thread::available_parallelism()
                .expect("failed to get available parallelism")
                .saturating_mul(NonZeroUsize::new(2).expect("2 is not zero"))
        }),
        on_error: &|path, err| eprintln!("{}: {}", path.display(), err),
    };

    let hist = filestat::traverse_dir_stat(&cli.root, &config);

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
            .map(|&size| (size, hist.quantile_below(size)))
            .collect(),
    };

    if cli.json {
        let out = miniserde::json::to_string(&out);
        println!("{out}");
    } else {
        println!("{out}");
    }
}
