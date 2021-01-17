use std::{convert::TryInto, fs::File, io::{Error, ErrorKind, Read, Seek, SeekFrom, Write}, path::PathBuf, sync::{Arc, atomic::{AtomicBool, Ordering}}};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::*;
use parking_lot::Mutex;
use reader::Reader;
use serde::{Deserialize, Serialize};

use descriptor::BucketDescription;

use crate::utils::{self, pool::Pool};

use self::{document::Document, writer::Writer};

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
    pub(crate) name: &'a str,
    pub(crate) file: File,
    pub(crate) path: PathBuf,
    pub(crate) descriptor: Option<Pool<BucketDescription>>,
    pub(crate) will_write: Arc<AtomicBool>,
    pub(crate) readers: Pool<Reader<'a>>,
    pub(crate) writer: Arc<Mutex<Writer<'a>>>,
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
        let mut bucket = Self {
            name,
            file,
            path,
            descriptor: None,
            will_write: will_write.clone(),
            readers: Pool::new(num_cpus::get(), || Reader::new(name, will_write.clone())),
            writer: Arc::new(Mutex::new(Writer::new(name, will_write)))
        };

        println!("Initializing bucket {} with path {:?}", bucket.name, bucket.path);
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
        let stats = fs2::statvfs(self.path.to_owned())?;
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
            self.descriptor = Some(Pool::new(num_cpus::get(), || descriptor.clone().unwrap()));
        }

        // Initialize the page and write it to disk
        self.initialize_page()?;

        Ok(())
    }

    /// ### Initializes a page with the following structure
    ///
    /// `Length of BucketDescription` as u16
    ///
    /// `BucketDescription`
    ///
    /// `Rows` are written below this
    pub fn initialize_page(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.file.seek(SeekFrom::Start(0))?;

        // Writes the descriptor to disk (WARN: Takes up a whol page)
        let buf;
        {
            let p = self.descriptor.as_ref().unwrap().pull();
            let r = p.as_ref();
            let mut d = bincode::serialize(r)?;

            let len = page_size::get() - d.len();
            let mut append = Vec::with_capacity(len);
            unsafe { append.set_len(len) };
            d.append(&mut append);

            buf = d;
        }

        let buf = buf.as_slice();
        self.file
            .write_u16::<LittleEndian>(buf.len().try_into().unwrap())?;
        self.file.write(buf)?;
        self.set_offset(page_size::get().try_into().unwrap())?;

        Ok(())
    }

    /// Load an already existing page from a bucket
    pub fn load_page(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.file.seek(SeekFrom::Start(0))?;

        let len = self.file.read_u16::<LittleEndian>()?;
        let mut buf = Vec::with_capacity(len.into());
        unsafe { buf.set_len(len.into()) };
        self.file.read(&mut buf)?;

        self.descriptor = Some(Pool::new(num_cpus::get(), || {
            bincode::deserialize::<BucketDescription>(buf.as_slice()).unwrap()
        }));
        Ok(())
    }

    /// Get the current offset for next document
    pub fn get_offset(&mut self) -> std::io::Result<u64> {
        let offset = (page_size::get() - std::mem::size_of::<u64>() * 2)
            .try_into()
            .unwrap();
        self.file.seek(SeekFrom::Start(offset))?;
        let val = self.file.read_u64::<LittleEndian>()?;
        Ok(val)
    }

    /// Sets the offset for next document
    pub fn set_offset(&mut self, offset: u64) -> std::io::Result<()> {
        let location = (page_size::get() - std::mem::size_of::<u64>() * 2)
            .try_into()
            .unwrap();
        self.file.seek(SeekFrom::Start(location))?;
        self.file.write_u64::<LittleEndian>(offset)?;
        Ok(())
    }

    /// Insert a document into the store
    pub fn insert(&mut self, document: &Document) -> Result<(), Box<dyn std::error::Error>> {
        let offset = self.get_offset()?;
        self.file.seek(SeekFrom::Start(offset))?;

        // Serialize document
        let mut buf = document.serialize()?;
        let len = utils::numbers::round_to_multiple(buf.len(), 8);
        buf.resize(len, 0);
        let slice = buf.as_slice();

        // Calculate new offset
        let new_offset = slice.len() as u64 + offset;

        // Set the state to write, dissallow from reading
        self.will_write.swap(true, Ordering::Acquire);

        // Write document
        self.file.write(&buf)?;
        self.set_offset(new_offset)?;
        let of = self.get_offset()?;

        // Todo: Implement indexing!

        // Release the lock, allow writing access
        self.will_write.swap(true, Ordering::Release);

        Ok(())
    }

    /// Initializes a new index for a field
    pub fn create_index() {}

    pub fn insert_into_index() {}
}

unsafe impl<'a> Send for Bucket<'a> {}
unsafe impl<'a> Sync for Bucket<'a> {}