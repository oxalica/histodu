use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use bytesize::ByteSize;
use clap::Parser;
use hdrhistogram::Histogram;
use miniserde::Serialize;
use walkdir::WalkDir;

#[derive(Debug, clap::Parser)]
#[command(version, about)]
struct Cli {
    /// Prevent traversing into other file systems.
    #[arg(long)]
    one_file_system: bool,

    /// Follow the root path if it is a symlink.
    /// Note that deep symlinks during traversal are never followed.
    #[arg(long)]
    follow_root: bool,

    /// Include all directories.
    #[arg(long)]
    include_dir: bool,

    /// Inlcude all zero-length files, or directories when `--include-dir` is enabled.
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
    let mut hist = Histogram::<u64>::new(3).unwrap();
    for ent in WalkDir::new(cli.root)
        .follow_links(false)
        .follow_root_links(cli.follow_root)
        .same_file_system(cli.one_file_system)
    {
        let ent = match ent {
            Ok(ent) => ent,
            Err(err) => {
                match err.path() {
                    Some(path) => eprintln!("fail to traverse {}: {}", path.display(), err),
                    None => eprintln!("fail to traverse: {err}"),
                }
                continue;
            }
        };
        if !cli.include_dir && ent.file_type().is_dir() {
            continue;
        }
        let metadata = match ent.metadata() {
            Ok(metadata) => metadata,
            Err(err) => {
                eprintln!(
                    "failed to get metadata of {}: {}",
                    ent.path().display(),
                    err
                );
                continue;
            }
        };
        let size = metadata.len();
        if !cli.include_empty && size == 0 {
            continue;
        }
        hist.record(size).unwrap();
    }

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
