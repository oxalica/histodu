use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::{fmt, process};

use bytesize::ByteSize;
use clap::Parser;
use hdrhistogram::Histogram;
use io_uring::types::Fd;
use io_uring::{opcode, IoUring};
use miniserde::Serialize;
use rayon::Scope;
use rustix::cstr;
use rustix::fd::{AsRawFd, FromRawFd, OwnedFd};
use rustix::fs::{
    openat, AtFlags, FileType, Mode, OFlags, RawDir, RawDirEntry, Statx, StatxFlags, CWD,
};
use rustix::io::Errno;

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

const IO_URING_ENTRIES: usize = 64;
const BUF_MASK_ALL: u64 = !0;
const INIT_DIR_BUF_LEN: usize = 4 << 10; // 4KiB

scoped_tls::scoped_thread_local!(static WORKER: RefCell<Worker>);

struct Worker {
    uring: IoUring,
    bufs: [Statx; IO_URING_ENTRIES],
    params: [Option<(RawFd, CString)>; IO_URING_ENTRIES],
    active_mask: u64,
    recorder: hdrhistogram::sync::Recorder<u64>,
    include_empty: bool,
    dirent_buf: Vec<u8>,
}

// Pending operations must be completed before dropping the buffer.
impl Drop for Worker {
    fn drop(&mut self) {
        if self.active_mask != 0 {
            eprintln!("tasks still active: {:b}", self.active_mask);
            std::process::abort();
        }
    }
}

impl Worker {
    fn new(recorder: hdrhistogram::sync::Recorder<u64>, include_empty: bool) -> Self {
        const NONE: Option<(RawFd, CString)> = None; // Workaround of const blocks.
        let uring = IoUring::new(IO_URING_ENTRIES.try_into().unwrap()).unwrap();
        Self {
            uring,
            bufs: [unsafe { std::mem::zeroed() }; IO_URING_ENTRIES],
            params: [NONE; IO_URING_ENTRIES],
            active_mask: 0,
            recorder,
            include_empty,
            dirent_buf: Vec::with_capacity(INIT_DIR_BUF_LEN),
        }
    }

    // # Safety
    // `dirfd` must be valid until the operation finished.
    unsafe fn enqueue(&mut self, dirfd: RawFd, ent: RawDirEntry<'_>, s: &Scope<'_>) {
        if ent.file_name() == cstr!(".") || ent.file_name() == cstr!("..") {
            return;
        }

        // If full, flush and wait.
        if self.active_mask == BUF_MASK_ALL {
            self.submit_and_complete(1, s);
        }

        // Always allocate a buffer, even for directories, since we always need to keep
        // parameters stored and alive.
        let buf_idx = self.active_mask.trailing_ones() as usize;
        assert!(buf_idx < IO_URING_ENTRIES);
        self.active_mask |= 1 << buf_idx;

        // Take ownership of parameters.
        let (_, filename) = self.params[buf_idx].insert((dirfd, ent.file_name().to_owned()));
        let dirfd = Fd(dirfd);
        let filename = filename.as_ptr();

        let op = if ent.file_type() != FileType::Directory {
            // File.
            let buf = &mut self.bufs[buf_idx];
            buf.stx_mask = StatxFlags::SIZE.bits();
            opcode::Statx::new(dirfd, filename, (buf as *mut Statx).cast())
                .flags(AtFlags::SYMLINK_NOFOLLOW.bits() as i32)
                .build()
                .user_data(buf_idx as u64)
        } else {
            // Directory.
            opcode::OpenAt::new(dirfd, filename)
                .flags(
                    (OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW).bits()
                        as _,
                )
                .mode(Mode::empty().bits())
                .build()
                .user_data(!(buf_idx as u64))
        };
        unsafe {
            self.uring.submission().push(&op).unwrap();
        }
    }

    fn submit_and_complete(&mut self, want: usize, s: &Scope<'_>) {
        self.uring.submit_and_wait(want).unwrap();
        for ent in self.uring.completion() {
            let data = ent.user_data() as i64;
            let buf_idx = if data >= 0 { data } else { !data } as usize;
            assert!(buf_idx < IO_URING_ENTRIES);
            self.active_mask ^= 1 << buf_idx;

            let (dirfd, filename) = self.params[buf_idx].take().unwrap();

            if ent.result() >= 0 {
                if data >= 0 {
                    // File `statx`.
                    let sz = self.bufs[buf_idx].stx_size;
                    if self.include_empty || sz != 0 {
                        self.recorder.record(sz).unwrap();
                    }
                } else {
                    // Directory `openat`.
                    let fd = unsafe { OwnedFd::from_raw_fd(ent.result()) };
                    s.spawn(move |s| WORKER.with(|w| w.borrow_mut().traverse_dir(fd, s)));
                }
            } else {
                let err = std::io::Error::from_raw_os_error(-ent.result());
                let ret = std::fs::read_link(format!("/proc/self/fd/{}", dirfd.as_raw_fd()));
                let dir_path = ret
                    .as_ref()
                    .map_or("<unknown>".into(), |path| path.to_string_lossy());
                eprintln!(
                    "failed to open {}/{}: {}",
                    dir_path,
                    filename.to_string_lossy(),
                    err,
                );
            }
        }
    }

    fn traverse_dir(&mut self, dirfd: OwnedFd, s: &Scope<'_>) {
        struct AbortOnPanic;
        impl Drop for AbortOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    eprintln!("panicking breaks safety invariants, aborting");
                    std::process::abort();
                }
            }
        }
        let _abort_on_panic = AbortOnPanic;

        // FIXME: one-file-system
        let mut dirent_buf = std::mem::take(&mut self.dirent_buf);
        'done: loop {
            'resize: {
                let mut iter = RawDir::new(&dirfd, dirent_buf.spare_capacity_mut());
                while let Some(entry) = iter.next() {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(Errno::INVAL) => break 'resize,
                        Err(err) => {
                            eprintln!("fail to traverse {:?}: {}", dirfd, err);
                            break 'done;
                        }
                    };
                    // SAFETY: Task completion is enforce below before dropping `dirfd`.
                    unsafe {
                        self.enqueue(dirfd.as_raw_fd(), entry, s);
                    }
                    while !iter.is_buffer_empty() {
                        let entry = iter.next().unwrap().unwrap();
                        // SAFETY: Task completion is enforce below before dropping `dirfd`.
                        unsafe {
                            self.enqueue(dirfd.as_raw_fd(), entry, s);
                        }
                    }
                    self.uring.submit().unwrap();
                }
                break 'done;
            }
            dirent_buf.reserve(self.dirent_buf.capacity() * 2);
        }

        self.dirent_buf = dirent_buf;

        // Force task completion.
        let pendings = self.active_mask.count_ones() as usize;
        if pendings != 0 {
            self.submit_and_complete(pendings, s);
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let mut hist = Histogram::<u64>::new(3).unwrap().into_sync();

    let rootfd = {
        let mut flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        flags.set(OFlags::NOFOLLOW, !cli.follow_root);
        match openat(CWD, &cli.root, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(err) => {
                eprintln!("failed to open {}: {}", cli.root.display(), err);
                process::exit(1);
            }
        }
    };

    rayon::ThreadPoolBuilder::new()
        .build_scoped(
            |thread| {
                let w = RefCell::new(Worker::new(hist.recorder(), cli.include_empty));
                WORKER.set(&w, || thread.run());
            },
            |pool| {
                pool.scope(|s| WORKER.with(|w| w.borrow_mut().traverse_dir(rootfd, s)));
            },
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
