use std::{convert::TryInto, fs::{File, OpenOptions}, io::{Seek, SeekFrom}, path::Path, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}};

use byteorder::{LittleEndian, ReadBytesExt};
use parking_lot::{Mutex, RawMutex, lock_api::MutexGuard};

#[derive(Clone, Debug)]
pub struct Reader<'a> {
    name: &'a str,
    file: Arc<Mutex<File>>,
    will_write: Arc<AtomicBool>,
    offset: Option<Arc<AtomicUsize>>,
}

impl<'a> Reader<'a> {
    pub fn new(name: &'a str, path: &Path, will_write: Arc<AtomicBool>, offset: Option<Arc<AtomicUsize>>) -> Result<Reader<'a>, Box<dyn std::error::Error>> {
        let file = OpenOptions::new().read(true).open(&path)?;
        let reader = Reader {
            name,
            file: Arc::new(Mutex::new(file)),
            will_write,
            offset
        };
        
        Ok(reader)
    }

    pub fn borrow_file(&mut self) -> MutexGuard<RawMutex, File> {
        self.file.lock()
    }

    /// Get the current offset for next document
    pub fn get_offset(&mut self) -> std::io::Result<u64> {
        if self.offset.is_some() {
            let of = self.offset.as_ref().unwrap();
            let offset = of.load(Ordering::SeqCst);
            return Ok(offset as u64);
        }

        let offset = (page_size::get() - std::mem::size_of::<u64>() * 2)
            .try_into()
            .unwrap();

        let mut f = self.borrow_file();
        f.seek(SeekFrom::Start(offset))?;
        let val = f.read_u64::<LittleEndian>()?;

        Ok(val)
    }
}

unsafe impl<'a> Send for Reader<'a> {}
unsafe impl<'a> Sync for Reader<'a> {}
