use std::{convert::TryInto, fs::{File, OpenOptions}, io::{Seek, SeekFrom, Write}, mem::MaybeUninit, path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread::JoinHandle};

use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_queue::ArrayQueue;
use log::trace;
use parking_lot::Mutex;

use crate::utils::threading::BooleanSemaphore;

// Information about the writer thread
#[derive(Debug, Clone)]
pub struct WriterThread {
    pub(crate) join_handle: Option<Arc<JoinHandle<QueuedWriter>>>,
    pub(crate) should_exit: Arc<AtomicBool>,
    pub(crate) q: Arc<ArrayQueue<QueuedWriteInformation>>,

    // Debugging
    pub(crate) items: Arc<AtomicUsize>,}

/// Data used to describe where the data will be written to
#[derive(Debug, Clone)]
pub struct QueuedWriteInformation {
    pub(crate) seek: (u64, u64),
    pub(crate) len: usize,
    pub(crate) bytes: Vec<u8>,
}

/// A threaded writer which chunks for faster writing
///
/// Chunks together multiple sequential buffers into one bigger buffer
pub struct QueuedWriter {
    pub(crate) q: Arc<ArrayQueue<QueuedWriteInformation>>,
    pub(crate) file: File,
    pub(crate) should_exit: Arc<AtomicBool>,

    // Debugging
    pub(crate) items: Arc<AtomicUsize>,
}

impl QueuedWriter {
    /// Creates a new QueuedWriter
    pub fn new(
        path: PathBuf,
        q: Arc<ArrayQueue<QueuedWriteInformation>>,
        should_exit: Arc<AtomicBool>,
    ) -> (QueuedWriter, WriterThread) {
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("Failed to open writer thread");

        let items = Arc::new(AtomicUsize::new(0));

        (
            QueuedWriter {
                q: q.clone(),
                file,
                should_exit: should_exit.clone(),
                items: items.clone(),
            },

            WriterThread {
                join_handle: None,
                should_exit,
                q,
                items,
            }
        )
    }

    /// Initializes and starts the writer
    ///
    /// Prepares it for writing
    pub fn start(&mut self, sleep_ns: u64) {
        // Todo: Implement some type of system to skip the while loop, as it's a big resource hog (works really well though)
        while !self.should_exit.as_ref().load(Ordering::SeqCst) || self.q.len() > 0 {
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
                        self.items.store(0, Ordering::SeqCst);
                        continue;
                    }
                };
            }

            // Check data length and sort by key to chunk
            if data.len() == 0 {
                self.items.store(0, Ordering::SeqCst);
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

            self.items.store(amount_chunked, Ordering::SeqCst);

            let el = t.elapsed();
            trace!(
                "Writes that where chunked: {} | Time to chunk: {:?}",
                amount_chunked,
                el
            );
        }
    }

    /// Store chunks to disk
    fn write_chunk(&mut self, chunk: &(u64, Vec<u8>)) -> Result<(), Box<dyn std::error::Error>> {
        let t = std::time::Instant::now();
        self.file.seek(SeekFrom::Start(chunk.0))?;
        self.file.write(&chunk.1)?;

        let location: u64 = (page_size::get() - std::mem::size_of::<u64>() * 2)
            .try_into()
            .unwrap();

        // Write the offset to disk
        // ! This does not work with multiple QueuedWriters as it does not keep track if the offset is 
        // ! larger than the old offset or not
        let offset = chunk.0 + chunk.1.len() as u64;
        self.file.seek(SeekFrom::Start(location))?;
        self.file.write_u64::<LittleEndian>(offset)?;

        let el = t.elapsed();
        trace!("Wrote chunks {:?} to disk with seek {} and length {}", el, chunk.0, chunk.1.len());
        Ok(())
    }
}
