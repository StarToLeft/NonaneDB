use std::{
    convert::TryInto,
    fs::File,
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle, Thread},
};

use crossbeam_queue::ArrayQueue;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::*;
use parking_lot::Mutex;
use reader::Reader;
use serde::{Deserialize, Serialize};

use descriptor::BucketDescription;

use crate::utils::{self, pool::Pool};

use self::{document::Document, writer::{Writer, queued::{QueuedWriteInformation, QueuedWriter, WriterThread}}};

pub mod descriptor;
pub mod document;
pub mod reader;
pub mod writer;

/// A minimum set of space required to initialize a bucket
///
/// The database does not need this much space to initialize a bucket.
/// However, to store any data with meaning it's good to have it.
static MIN_FREE_BYTES: u64 = 1_048_576; // A minimum of 1 MB of free space

/// A bucket defines a datastructure, it contains a whole database within it
pub struct Bucket<'a> {
    pub(crate) name: Arc<&'a str>,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) descriptor: Arc<Option<Pool<BucketDescription>>>,
    pub(crate) will_write: Arc<AtomicBool>,
    pub(crate) readers: Arc<Pool<Reader<'a>>>,
    pub(crate) writer: Arc<Mutex<Writer<'a>>>,
    pub(crate) writer_thread: WriterThread,
}

impl<'a> Bucket<'a> {
    /// Creates a new bucket and initializes it with it's required data structure
    pub fn new(
        name: &'a str,
        file: File,
        path: PathBuf,
        should_init: bool,
        descriptor: Option<BucketDescription>,
    ) -> Result<Bucket<'a>, Box<dyn std::error::Error>> {
        let will_write = Arc::new(AtomicBool::new(false));
        
        // Initialize multi-readers
        let readers = Pool::new(num_cpus::get(), || {
            Reader::new(name, &path.clone(), will_write.clone())
                .expect("Failed to initialize reader for pool")
        });

        // Initialize single writer
        // Todo: Add QueuedWriter for multi-threaded writing
        // Todo: Will optimize by combining bytes to be written into a big insert instead of a lot of small ones
        let writer = Arc::new(Mutex::new(
            Writer::new(name, &path.clone(), will_write.clone())
                .expect("Failed to initialize writer for bucket"),
        ));

        // Initialize write queue
        let q: ArrayQueue<QueuedWriteInformation> = ArrayQueue::new(1000);
        let thread = thread::Builder::new()
            .name(name.into())
            .spawn(|| {
                QueuedWriter::new(q)
            })
            .unwrap();

        // Create bucket
        let mut bucket = Self {
            name: Arc::new(name),
            path: Arc::new(path.clone()),
            descriptor: Arc::new(None),
            readers: Arc::new(readers),
            writer,
            will_write,
            writer_thread: WriterThread {
                join_handle: Arc::new(thread),
            }
        };

        trace!(
            "Initializing and opening readers and writer for bucket {}",
            bucket.name
        );
        if should_init {
            bucket.initialize(descriptor)?;
        } else {
            bucket.load_page()?;
        }

        Ok(bucket)
    }

    pub fn initialize(
        &mut self,
        descriptor: Option<BucketDescription>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Check if there are enough bytes of free space to run a database
        let stats = fs2::statvfs(self.path.as_ref())?;
        if stats.free_space() < MIN_FREE_BYTES {
            return Err(Box::new(Error::new(
                ErrorKind::AlreadyExists,
                "Out of free space to initialize bucket",
            )));
        }

        // Check if the descriptor is defined
        if descriptor.is_none() {
            panic!(
                "Couldn't initialize bucket due to descriptor not defined {}",
                self.path.file_name().unwrap().to_str().unwrap()
            );
        } else {
            self.descriptor = Arc::new(Some(Pool::new(num_cpus::get(), || {
                descriptor.clone().unwrap()
            })));
        }

        // Initialize the page and write it to disk
        self.initialize_page()?;

        Ok(())
    }

    /// ### Lock the writer and prepare for insert
    /// Must be called before writing to a file as it will otherwise affect performance for reads
    /// writes without calling this might error other reads
    pub fn toggle_writer(&mut self) {
        if self.will_write.load(Ordering::Relaxed) {
            self.will_write.swap(true, Ordering::Relaxed);
        } else {
            self.will_write.swap(false, Ordering::Relaxed);
        }
    }

    /// ### Initializes a page with the following structure
    ///
    /// `Length of BucketDescription` as u16
    ///
    /// `BucketDescription`
    ///
    /// `Rows` are written below this
    pub fn initialize_page(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // self.file.as_ref().seek(SeekFrom::Start(0))?;

        // Writes the descriptor to disk (WARN: Takes up a whol page)
        let buf;
        {
            let p = self.descriptor.as_ref().as_ref().unwrap().pull();
            let r = p.as_ref();
            let mut d = bincode::serialize(r)?;

            let len = page_size::get() - d.len();
            let mut append = Vec::with_capacity(len);
            unsafe { append.set_len(len) };
            d.append(&mut append);

            buf = d;
        }

        let buf = buf.as_slice();
        self.toggle_writer();
        {
            let mut wrt = self.writer.lock();
            let file = wrt.borrow_file();
            file.write_u16::<LittleEndian>(buf.len().try_into().unwrap())?;
            file.write(buf)?;
            wrt.set_offset(page_size::get().try_into().unwrap())?;
        }
        self.toggle_writer();

        Ok(())
    }

    /// Load an already existing page from a bucket
    pub fn load_page(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut reader = self.readers.pull();
        let reader = reader.as_mut_ref();
        let mut file = reader.borrow_file();
        file.seek(SeekFrom::Start(0))?;

        let len = file.read_u16::<LittleEndian>()?;
        let mut buf = Vec::with_capacity(len.into());
        unsafe { buf.set_len(len.into()) };
        file.read(&mut buf)?;

        self.descriptor = Arc::new(Some(Pool::new(num_cpus::get(), || {
            bincode::deserialize::<BucketDescription>(buf.as_slice()).unwrap()
        })));
        Ok(())
    }

    /// Insert a document into the store
    pub fn insert(
        &mut self,
        document: &Document,
    ) -> Result<(usize, [u8; 24]), Box<dyn std::error::Error>> {
        let offset = self.readers.pull().as_mut_ref().get_offset()?;

        // Serialize documentappend
        let mut buf = document.serialize()?;
        // Todo: Change to constant across whole DB
        let len = utils::numbers::round_to_multiple(buf.len(), 8);
        buf.resize(len, 0);
        let slice = buf.as_slice();

        // Calculate new offset
        let new_offset = slice.len() as u64 + offset + std::mem::size_of::<u64>() as u64;

        // Toggle the writer
        // Todo: Replace Try (?) with match to handle writing errors
        // ! Not a big issue right now, but eventually it will become one 🚀
        self.toggle_writer();
        {
            // Fetch the writer
            let mut wrt = self.writer.lock();
            let file = wrt.borrow_file();
            file.seek(SeekFrom::Start(offset))?;
            file.write_u64::<LittleEndian>(buf.len() as u64)?;

            // Write document
            file.write(&buf)?;
            wrt.set_offset(new_offset)?;

            // Todo: Implement indexing!

            // Todo: Handle events with file.sync_all()
        }
        self.toggle_writer();

        Ok((new_offset as usize, [0; 24]))
    }

    /// Initializes a new index for a field
    pub fn create_index() {}

    pub fn insert_into_index() {}
}

impl<'a> Clone for Bucket<'a> {
    fn clone(&self) -> Self {
        unsafe {
            std::mem::transmute_copy(self)
        }
    }
}

unsafe impl<'a> Send for Bucket<'a> {}
unsafe impl<'a> Sync for Bucket<'a> {}
