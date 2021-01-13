use std::{
    fs::File,
    io::{Error, ErrorKind, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use byteorder::{LittleEndian, WriteBytesExt};
use fs2::*;

pub mod descriptor;
pub mod document;

/// A minimum set of space required to initialize a bucket
///
/// The database does not need this much space to initialize a bucket.
/// However, to store any data with meaning it's good to have it.
static MIN_FREE_BYTES: u64 = 1_048_576; // A minimum of 1 MB of free space

/// A bucket defines a datastructure, it contains a whole database within it
pub struct Bucket {
    file: File,
    path: PathBuf,
}

impl Bucket {
    /// Creates a new bucket and initializes it with it's required data structure
    pub fn new(
        file: File,
        path: PathBuf,
        should_init: bool,
    ) -> std::io::Result<Arc<Mutex<Bucket>>> {
        file.lock_exclusive()?;

        let mut bucket = Self {
            file,
            path,
        };
        if should_init {
            bucket.initialize()?;
        }

        Ok(Arc::new(Mutex::new(bucket)))
    }

    pub fn initialize(&mut self) -> std::io::Result<()> {
        // Check if there are enough bytes of free space to run a database
        let stats = fs2::statvfs(self.path.to_owned())?;
        if stats.free_space() < MIN_FREE_BYTES {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "Out of free space to initialize bucket",
            ));
        }

        // Allocated the needed space to initialize the bucket
        self.file.allocate(MIN_FREE_BYTES)?;

        Ok(())
    }

    /// ### Initializes a page with the following structure
    ///
    /// `Length of BucketDescription` as u16
    ///
    /// `BucketDescription`
    ///
    /// `Rows` are written below this
    pub fn initialize_page(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;

        let len = 0;
        self.file.write_u16::<LittleEndian>(len)?;

        Ok(())
    }
}
