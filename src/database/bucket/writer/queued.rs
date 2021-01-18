use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    mem::MaybeUninit,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use crossbeam_queue::ArrayQueue;
use log::trace;
use parking_lot::Mutex;

use crate::utils::threading::BooleanSemaphore;

// Information about the writer thread
#[derive(Debug, Clone)]
pub struct WriterThread {
    pub(crate) join_handle: Arc<JoinHandle<QueuedWriter>>,
    pub(crate) should_exit: Arc<AtomicBool>,
    pub(crate) q: Arc<ArrayQueue<QueuedWriteInformation>>,
}

/// Data used to describe where the data will be written to
#[derive(Debug, Clone)]
pub struct QueuedWriteInformation {
    pub(crate) seek: (u64, u64),
    pub(crate) len: usize,
    pub(crate) bytes: Vec<u8>,
}

pub struct QueuedWriter {
    pub(crate) q: Arc<ArrayQueue<QueuedWriteInformation>>,
    pub(crate) file: File,
    pub(crate) should_exit: Arc<AtomicBool>,
}

impl QueuedWriter {
    /// Creates a new QueuedWriter
    pub fn new(
        path: PathBuf,
        q: Arc<ArrayQueue<QueuedWriteInformation>>,
        should_exit: Arc<AtomicBool>,
    ) -> QueuedWriter {
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("Failed to open writer thread");
        QueuedWriter {
            q,
            file,
            should_exit,
        }
    }

    /// Initializes and starts the writer
    pub fn start(&mut self, sleep_ns: u64) {
        // Todo: Implement some type of system to skip the while loop, as it's a big resource hog (works really well though)
        while !self.should_exit.as_ref().load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_nanos(sleep_ns));
            let t = std::time::Instant::now();
            let l = self.q.len().max(25);
            let mut data = Vec::with_capacity(l);
            for i in 0..l {
                let el = self.q.pop();
                let el = match el {
                    Some(el) => {
                        data.push((el.seek, el));
                    }
                    None => {
                        continue;
                    }
                };
            }
            if data.len() == 0 {
                continue;
            } else {
                data.sort_unstable_by_key(|x| x.0);
            }

            // Find data that can be written sequentially
            let mut amount_chunked = 0;
            let mut chunk: (u64, Vec<u8>) = (data.first().unwrap().0 .0, Vec::new());
            let mut last_offset: i64 = data.first().unwrap().0 .0 as i64;
            for d in data.iter_mut() {
                if d.0 .0 as i64 == last_offset || last_offset == -1 {
                    // Add initial seek if it's the first insert
                    if chunk.1.len() == 0 {
                        chunk.0 = d.0 .0;
                    }

                    // Add the bytes of the data
                    chunk.1.append(&mut d.1.bytes);
                    last_offset = d.0 .1 as i64;
                    amount_chunked += 1;
                } else {
                    let res = self.write_chunk(&chunk);
                    match res {
                        Ok(_) => {}
                        Err(_) => {
                            error!("Failed to write chunks");
                        }
                    }
                    chunk = (0, Vec::new());
                    last_offset = -1;
                }
            }

            // Try to write any data that was "forgotten"
            if chunk.1.len() > 0 {
                let res = self.write_chunk(&chunk);
                match res {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Error with writing chunks {:?}", e);
                    }
                }
            }

            let el = t.elapsed();
            trace!(
                "Writes that where chunked: {} | Time to chunk: {:?}",
                amount_chunked,
                el
            );
        }
    }

    fn write_chunk(&mut self, chunk: &(u64, Vec<u8>)) -> Result<(), Box<dyn std::error::Error>> {
        let t = std::time::Instant::now();
        self.file.seek(SeekFrom::Start(chunk.0))?;
        self.file.write(&chunk.1)?;
        let el = t.elapsed();
        trace!("Wrote chunks {:?} to disk with seek {}", el, chunk.0);
        Ok(())
    }
}
