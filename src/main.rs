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

    /// Print approximated values at specific quantiles.
    /// The value is given in integer percentage in range [0, 100].
    #[arg(
        long,
        short = 'q',
        default_values = ["0", "50", "90", "99", "100"],
        value_parser = clap::value_parser!(u8).range(0..=100)
    )]
    at_quantile: Vec<u8>,

    /// Print approximated quantiles below specific values.
    /// The value can be given as an integer in bytes, or with an SI or binary suffix.
    #[arg(long, short = 'r', default_values = ["4KiB", "64KiB", "1MiB"])]
    quantile_of: Vec<ByteSize>,

    /// Print output in JSON format.
    #[arg(long)]
    json: bool,

    /// The root path to search.
    root_path: PathBuf,

    /// The maximal concurrency. If set to zero, the effective value is
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

    let hist = filestat::traverse_dir_stat(&cli.root_path, &config);

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
