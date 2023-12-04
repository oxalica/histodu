use std::collections::BTreeMap;
use std::ffi::CString;
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;
use std::{fmt, thread};

use bytesize::ByteSize;
use clap::Parser;
use crossbeam::channel::Receiver;
use hdrhistogram::Histogram;
use io_uring::types::Fd;
use io_uring::{opcode, IoUring, SubmissionQueue};
use miniserde::Serialize;

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

const IO_URING_ENTRIES: usize = 32;

struct Worker {
    bufs: [libc::statx; IO_URING_ENTRIES],
    paths: [Option<CString>; IO_URING_ENTRIES],
    active_mask: u64,
    recorder: hdrhistogram::sync::Recorder<u64>,
    include_empty: bool,
}

impl Worker {
    fn new(recorder: hdrhistogram::sync::Recorder<u64>, include_empty: bool) -> Self {
        const NONE: Option<CString> = None; // Workaround of const blocks.
        Self {
            bufs: [unsafe { std::mem::zeroed() }; IO_URING_ENTRIES],
            paths: [NONE; IO_URING_ENTRIES],
            active_mask: 0,
            recorder,
            include_empty,
        }
    }

    fn submit_one(&mut self, sub: &mut SubmissionQueue, path: PathBuf) -> bool {
        let buf_idx = self.active_mask.trailing_ones() as usize;
        assert!(buf_idx < IO_URING_ENTRIES);
        self.active_mask |= 1 << buf_idx;

        let mut path = path.into_os_string().into_vec();
        path.push(b'\0');
        let path = CString::from_vec_with_nul(path).unwrap();

        let dirfd = Fd(libc::AT_FDCWD);
        let pathname = path.as_c_str().as_ptr();
        let buf = &mut self.bufs[buf_idx] as *mut libc::statx;
        self.paths[buf_idx] = Some(path); // Keep the pathname string alive.
        unsafe {
            let op = &opcode::Statx::new(dirfd, pathname, buf.cast())
                .flags(libc::AT_SYMLINK_NOFOLLOW)
                .build()
                .user_data(buf_idx as u64);
            sub.push(op).unwrap();
        }

        self.active_mask == (1 << IO_URING_ENTRIES) - 1
    }

    fn handle_completed(&mut self, uring: &mut IoUring) {
        let compe = uring.completion();
        for ent in compe {
            let buf_idx = ent.user_data() as usize;
            assert!(buf_idx < IO_URING_ENTRIES);
            self.active_mask ^= 1 << buf_idx;

            if ent.result() == 0 {
                let sz = self.bufs[buf_idx].stx_size;
                if self.include_empty || sz != 0 {
                    self.recorder.record(sz).unwrap();
                }
            } else {
                let err = std::io::Error::from_raw_os_error(-ent.result());
                eprintln!(
                    "failed to get metadata of {}: {}",
                    self.paths[buf_idx].as_ref().unwrap().to_string_lossy(),
                    err,
                );
            }
        }
    }

    fn work(
        recorder: hdrhistogram::sync::Recorder<u64>,
        file_rx: Receiver<PathBuf>,
        include_empty: bool,
    ) {
        let mut uring = IoUring::new(IO_URING_ENTRIES.try_into().unwrap()).unwrap();
        let mut state = Self::new(recorder, include_empty);

        while let Ok(path) = file_rx.recv() {
            let mut sub = uring.submission();
            let mut is_full = state.submit_one(&mut sub, path);
            while !is_full {
                if let Ok(path) = file_rx.try_recv() {
                    is_full = state.submit_one(&mut sub, path);
                } else {
                    break;
                }
            }
            drop(sub);
            uring.submit_and_wait(if is_full { 1 } else { 0 }).unwrap();
            state.handle_completed(&mut uring);
        }

        let ongoing_cnt = state.active_mask.count_ones() as usize;
        if ongoing_cnt > 0 {
            uring.submit_and_wait(ongoing_cnt).unwrap();
            state.handle_completed(&mut uring);
        }
        assert_eq!(state.active_mask, 0);
    }
}

fn main() {
    let mut cli = Cli::parse();
    let mut hist = Histogram::<u64>::new(3).unwrap().into_sync();

    if cli.follow_root {
        cli.root = std::fs::canonicalize(cli.root).unwrap();
    }

    thread::scope(|scope| {
        let (file_tx, file_rx) = crossbeam::channel::unbounded::<PathBuf>();

        let threads = thread::available_parallelism().map_or(1, |n| n.get());
        for _ in 0..threads {
            let recorder = hist.recorder();
            let file_rx = file_rx.clone();
            scope.spawn(|| Worker::work(recorder, file_rx, cli.include_empty));
        }

        fn spawn_traverse(
            s: &rayon::Scope,
            path: PathBuf,
            file_tx: crossbeam::channel::Sender<PathBuf>,
        ) {
            s.spawn(move |s| {
                // FIXME: one-file-system
                for ent in std::fs::read_dir(&path).unwrap() {
                    let ent = match ent {
                        Ok(ent) => ent,
                        Err(err) => {
                            eprintln!("fail to traverse {}: {}", path.display(), err);
                            continue;
                        }
                    };
                    let file_path = ent.path();
                    if !ent.file_type().unwrap().is_dir() {
                        file_tx.send(file_path).unwrap();
                    } else {
                        spawn_traverse(s, file_path, file_tx.clone());
                    }
                }
            });
        }
        rayon::scope(|s| spawn_traverse(s, cli.root.clone(), file_tx));
    });

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
