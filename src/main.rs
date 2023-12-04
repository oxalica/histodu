use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use bytesize::ByteSize;
use clap::Parser;
use hdrhistogram::sync::Recorder;
use hdrhistogram::Histogram;
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

    let mut hist = Histogram::<u64>::new(3).unwrap().into_sync();

    scoped_tls::scoped_thread_local!(static LOCAL_RECORDER: RefCell<Recorder<u64>>);

    fn traverse(s: &rayon::Scope, path: PathBuf, include_empty: bool) {
        for ent in std::fs::read_dir(&path).unwrap() {
            let ent = match ent {
                Ok(ent) => ent,
                Err(err) => {
                    eprintln!("fail to traverse {}: {}", path.display(), err);
                    continue;
                }
            };
            if !ent.file_type().unwrap().is_dir() {
                s.spawn(move |_s| {
                    let sz = ent.metadata().unwrap().len();
                    if include_empty || sz != 0 {
                        LOCAL_RECORDER.with(|recorder| {
                            recorder.borrow_mut().record(sz).unwrap();
                        });
                    }
                })
            } else {
                let file_path = ent.path();
                s.spawn(move |s| traverse(s, file_path, include_empty));
            }
        }
    }

    let threads = if cli.threads != 0 {
        cli.threads
    } else {
        std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .saturating_mul(2)
    };
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_scoped(
            |thread| {
                let recorder = RefCell::new(hist.recorder());
                LOCAL_RECORDER.set(&recorder, || thread.run());
            },
            |pool| pool.scope(|s| traverse(s, cli.root.clone(), cli.include_empty)),
        )
        .unwrap();

    hist.refresh();

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
