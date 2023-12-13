//! WARNING: The library interface of this crate is considered unstable and
//! should not be relied on. The crate version is solely coresponding to the binary CLI.
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use hdrhistogram::sync::Recorder;
use hdrhistogram::{Histogram, SyncHistogram};

pub struct Config<'a> {
    pub include_empty: bool,
    pub threads: NonZeroUsize,
    pub on_error: &'a (dyn Fn(&Path, std::io::Error) + Sync),
}

pub fn dir_size_histogram(root_path: &Path, config: &Config<'_>) -> SyncHistogram<u64> {
    let mut hist = Histogram::new(3).expect("sigfig 3 is valid").into_sync();
    rayon::ThreadPoolBuilder::new()
        .num_threads(config.threads.get())
        .build_scoped(
            |thread| {
                let recorder = RefCell::new(hist.recorder());
                LOCAL_RECORDER.set(&recorder, || thread.run());
            },
            |pool| pool.scope(|s| traverse_dir(s, root_path, config)),
        )
        .expect("failed to build rayon runtime");
    // All recorders should already died.
    hist.refresh_timeout(Duration::ZERO);
    hist
}

scoped_tls::scoped_thread_local!(static LOCAL_RECORDER: RefCell<Recorder<u64>>);

fn traverse_dir<'s>(s: &rayon::Scope<'s>, path: &Path, config: &'s Config<'s>) {
    let emit = |err| (config.on_error)(path, err);
    let Ok(iter) = std::fs::read_dir(path).map_err(emit) else {
        return;
    };

    for ent in iter {
        let Ok(ent) = ent.map_err(emit) else {
            continue;
        };
        s.spawn(move |s| {
            let ret = (|| {
                if !ent.file_type()?.is_dir() {
                    let size = ent.metadata()?.len();
                    if config.include_empty || size != 0 {
                        LOCAL_RECORDER.with(|recorder| {
                            recorder
                                .borrow_mut()
                                .record(size)
                                .expect("auto-resize is enabled");
                        });
                    }
                } else {
                    let file_path = ent.path();
                    s.spawn(move |s| traverse_dir(s, &file_path, config));
                }
                Ok(())
            })();
            if let Err(err) = ret {
                (config.on_error)(&ent.path(), err);
            }
        });
    }
}
