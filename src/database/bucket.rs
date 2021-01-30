use std::{
    convert::TryInto,
    fs::File,
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, JoinHandle, Thread},
};

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use crossbeam_queue::ArrayQueue;
use fs2::*;
use parking_lot::Mutex;
use reader::Reader;
use serde::{Deserialize, Serialize};

use descriptor::BucketDescription;

use crate::utils::{self, pool::Pool};

use self::{
    document::Document,
    writer::{
        queued::{QueuedWriteInformation, QueuedWriter, WriterThread},
        Writer,
    },
};

pub mod descriptor;
pub mod document;
pub mod reader;
pub mod writer;

/// A minimum set of space required to initialize a bucket
///
/// The database does not need this much space to initialize a bucket.
/// However, to store any data with meaning it's good to have it.
static MIN_FREE_BYTES: u64 = 1_048_576; // A minimum of 1 MB of free space

#[derive(Clone)]
/// A bucket defines a datastructure, it contains a whole database within it
pub struct Bucket<'a> {
    pub(crate) name: Arc<&'a str>,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) descriptor: Arc<Option<Pool<BucketDescription>>>,
    pub(crate) will_write: Arc<AtomicBool>,
    pub(crate) readers: Option<Arc<Pool<Reader<'a>>>>,
    pub(crate) writer: Arc<Mutex<Writer<'a>>>,
    pub(crate) writer_thread: Option<WriterThread>,
    pub(crate) atomic_offset: Arc<AtomicUsize>,
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

        // Initialize single writer
        let writer = Arc::new(Mutex::new(
            Writer::new(name, &path.clone(), will_write.clone())
                .expect("Failed to initialize writer for bucket"),
        ));

        // Initialize write queue
        let should_exit = Arc::new(AtomicBool::new(false));
        let has_data: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let write_queue: ArrayQueue<QueuedWriteInformation> = ArrayQueue::new(10000);
        let write_queue = Arc::new(write_queue);

        // Clones to be used within WriteThread struct to handle multi threaded writes
        let write_queue_cl = write_queue.clone();
        let should_exit_cl = should_exit.clone();

        // Path for QueuedWriter to write at
        let p = path.clone();

        // Create bucket
        let mut bucket = Self {
            name: Arc::new(name),
            path: Arc::new(path.clone()),
            descriptor: Arc::new(None),
            readers: None,
            writer,
            will_write: will_write.clone(),
            writer_thread: None,
            atomic_offset: Arc::new(AtomicUsize::new(0)),
        };

        trace!(
            "Initializing and opening readers and writer for bucket {}",
            bucket.name
        );

        // Initialize and load bucket
        if should_init {
            bucket.initialize(descriptor)?;
        } else {
            bucket.load_page()?;
        }

        // Temporary reader to read initial offset
        let mut reader = Reader::new(name, &path.clone(), will_write.clone(), None)
            .expect("Failed to initialize reader for pool");
        let offset = reader.get_offset()? as usize;
        bucket.atomic_offset = Arc::new(AtomicUsize::new(offset));

        // Create the thread for writing for this bucket (and all clones of this bucket)
        let thread = thread::Builder::new()
            .name(name.into())
            .spawn(|| {
                let mut writer = QueuedWriter::new(p, write_queue, should_exit);
                writer.start(20);
                writer
            })
            .unwrap();

        // Assign thread data
        bucket.writer_thread = Some(WriterThread {
            join_handle: Arc::new(thread),
            should_exit: should_exit_cl,
            q: write_queue_cl,
        });

        // Initialize multi-readers
        let readers = Pool::new(num_cpus::get(), || {
            Reader::new(
                name,
                &path.clone(),
                will_write.clone(),
                Some(bucket.atomic_offset.clone()),
            )
            .expect("Failed to initialize reader for pool")
        });

        // Assign readers
        bucket.readers = Some(Arc::new(readers));

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
        if self.will_write.load(Ordering::SeqCst) {
            self.will_write.swap(true, Ordering::SeqCst);
        } else {
            self.will_write.swap(false, Ordering::SeqCst);
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
        trace!("Initializing initial page for bucket {}", self.name);

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
        // Create a temporary reader
        let mut reader = Reader::new(&self.name, &self.path, self.will_write.clone(), None)?;
        let mut file = reader.borrow_file();
        file.seek(SeekFrom::Start(0))?;

        // Read the descriptor length and then the bucket descriptor
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
        let offset = self
            .readers
            .as_ref()
            .unwrap()
            .pull()
            .as_mut_ref()
            .get_offset()?;

        // Buffer to be written to disk
        let mut buf = Vec::new();

        // Serialize document
        let mut serialized_data = document.serialize()?;

        // Add length of document to ease reading
        let mut len = Vec::new();
        len.write_u64::<LittleEndian>(serialized_data.len() as u64)?;

        buf.append(&mut len);
        buf.append(&mut serialized_data);
        // Todo: Change to constant across whole DB
        let len = utils::numbers::round_to_multiple(buf.len(), 8);
        buf.resize(len, 0);
        let slice = buf.as_slice();

        // Calculate new offset
        let new_offset = slice.len() as u64 + offset + std::mem::size_of::<u64>() as u64;

        // Todo: Replace Try (?) with match to handle writing errors
        // ! Not a big issue right now, but eventually it will become one 🚀
        self.atomic_offset
            .store(new_offset.try_into().unwrap(), Ordering::SeqCst);

        // Set up queued write object
        let info = QueuedWriteInformation {
            seek: (offset, new_offset),
            len: buf.len(),
            bytes: buf,
        };

        // Push it to the queue or error if it's full
        // (not very effiecent, however exceeding X amount of inserts per second might be a problem, time to add a new cluster)
        // Or I guess, if you're cool, add more ram
        let wrt_thrd = self.writer_thread.as_ref().unwrap();
        let res = wrt_thrd.q.push(info);
        match res {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::new(std::io::Error::new(
                    ErrorKind::TimedOut,
                    "Out of queue space",
                )));
            }
        }

        // Todo: Implement indexing!
        // Todo: Handle events with file.sync_all()
        Ok((new_offset as usize, [0; 24]))
    }

    pub fn count_documents(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let mut count = 0;

        // Borrow a reader
        let mut reader = self.readers.as_ref().unwrap().pull();
        let reader = reader.as_mut_ref();
        let mut file = reader.borrow_file();

        let mut offset = page_size::get() as u64;
        loop {
            file.seek(SeekFrom::Start(offset))?;
            let size = file.read_u64::<LittleEndian>();
            let mut size = match size {
                Ok(s) => s,
                Err(_) => {
                    break;
                }
            };

            size += std::mem::size_of::<u64>() as u64;
            offset = size;

            count += 1;
        }

        return Ok(count);
    }
}

unsafe impl<'a> Send for Bucket<'a> {}
unsafe impl<'a> Sync for Bucket<'a> {}
