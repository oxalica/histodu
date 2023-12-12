use std::path::PathBuf;

#[cfg(not_in_build_rs)]
use bytesize::ByteSize;
#[cfg(not(not_in_build_rs))]
use u64 as ByteSize;

#[derive(Debug, clap::Parser)]
#[command(about, version = option_env!("CFG_RELEASE").unwrap_or(env!("CARGO_PKG_VERSION")))]
pub struct Cli {
    /// Include all zero-length files.
    #[arg(long)]
    pub include_empty: bool,

    /// Print approximated values at specific quantiles.
    /// The value is given in integer percentage in range [0, 100].
    #[arg(
        long,
        short = 'q',
        default_values = ["0", "50", "90", "99", "100"],
        value_parser = clap::value_parser!(u8).range(0..=100)
    )]
    pub at_quantile: Vec<u8>,

    /// Print approximated quantiles below specific values.
    /// The value can be given as an integer in bytes, or with an SI or binary suffix.
    #[arg(long, short = 'r', default_values = ["4KiB", "64KiB", "1MiB"])]
    pub quantile_of: Vec<ByteSize>,

    /// Print output in JSON format.
    #[arg(long)]
    pub json: bool,

    /// The root directory to traverse.
    pub root_path: PathBuf,

    /// The maximal concurrency. If set to zero, the effective value is
    /// twice the number of logical CPUs.
    #[arg(long, default_value = "0")]
    pub threads: usize,
}
