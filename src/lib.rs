//! WARNING: The library interface of this crate is considered unstable and
//! should not be relied on. The crate version is solely coresponding to the binary CLI.
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use hdrhistogram::sync::Recorder;
use hdrhistogram::{Histogram, SyncHistogram};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub struct Config<'a> {
    pub one_file_system: bool,
    pub include_empty: bool,
    pub threads: NonZeroUsize,
    pub on_error: &'a (dyn Fn(&Path, std::io::Error) + Sync),
}

/// Traverse a root directory and gather statistics information of all file sizes.
///
/// # Error
/// Errors are reported via `Config::on_error`. In case of critical errors, it returns `Err(())`.
/// Otherwise, errors are reported and relevant files are skipped.
#[allow(clippy::result_unit_err)]
pub fn dir_size_histogram(root_path: &Path, config: &Config<'_>) -> Result<SyncHistogram<u64>, ()> {
    let emit = |err| (config.on_error)(root_path, err);

    let expect_dev_id = config
        .one_file_system
        .then(|| {
            #[cfg(unix)]
            {
                Ok(std::fs::metadata(root_path)?.dev())
            }

            #[cfg(not(unix))]
            {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "one-file-system filter is unsupported on this platform",
                ))
            }
        })
        .transpose()
        .map_err(emit)?;

    let mut hist = Histogram::new(3).expect("sigfig 3 is valid").into_sync();
    rayon::ThreadPoolBuilder::new()
        .num_threads(config.threads.get())
        .build_scoped(
            |thread| {
                let recorder = RefCell::new(hist.recorder());
                LOCAL_RECORDER.set(&recorder, || thread.run());
            },
            |pool| pool.scope(|s| traverse_dir(s, root_path, config, expect_dev_id)),
        )
        .expect("failed to build rayon runtime");
    // All recorders should already died.
    hist.refresh_timeout(Duration::ZERO);
    Ok(hist)
}

scoped_tls::scoped_thread_local!(static LOCAL_RECORDER: RefCell<Recorder<u64>>);

fn traverse_dir<'s>(
    s: &rayon::Scope<'s>,
    path: &Path,
    config: &'s Config<'s>,
    expect_dev_id: Option<u64>,
) {
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
                let (file_type, meta) = if let Some(expect_dev_id) = expect_dev_id {
                    #[cfg(unix)]
                    {
                        let meta = ent.metadata()?;
                        if meta.dev() != expect_dev_id {
                            return Ok(());
                        }
                        (meta.file_type(), Some(meta))
                    }

                    #[cfg(not(unix))]
                    {
                        let _ = expect_dev_id;
                        unreachable!()
                    }
                } else {
                    (ent.file_type()?, None::<std::fs::Metadata>)
                };

                if !file_type.is_dir() {
                    let size = match meta {
                        Some(meta) => meta.len(),
                        None => ent.metadata()?.len(),
                    };
                    if size == 0 && !config.include_empty {
                        return Ok(());
                    }
                    LOCAL_RECORDER.with(|recorder| {
                        recorder
                            .borrow_mut()
                            .record(size)
                            .expect("auto-resize is enabled");
                    });
                } else {
                    let file_path = ent.path();
                    s.spawn(move |s| traverse_dir(s, &file_path, config, expect_dev_id));
                }
                Ok(())
            })();
            if let Err(err) = ret {
                (config.on_error)(&ent.path(), err);
            }
        });
    }
}
