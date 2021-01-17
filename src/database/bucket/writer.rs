use std::{convert::TryInto, fs::{File, OpenOptions}, io::{Seek, SeekFrom}, path::Path, sync::{atomic::AtomicBool, Arc}};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub mod queued;

#[derive(Debug)]
pub struct Writer<'a> {
    pub(crate) name: &'a str,
    pub(crate) file: File,
    pub(crate) will_write: Arc<AtomicBool>,
}

impl<'a> Writer<'a> {
    pub fn new(
        name: &'a str,
        path: &Path,
        will_write: Arc<AtomicBool>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new().write(true).open(&path)?;
        let writer = Self {
            name,
            file,
            will_write,
        };

        Ok(writer)
    }

    pub fn borrow_file(&mut self) -> &mut File {
        &mut self.file
    }

    /// Sets the offset for next document
    pub fn set_offset(&mut self, offset: u64) -> std::io::Result<()> {
        let location: u64 = (page_size::get() - std::mem::size_of::<u64>() * 2)
            .try_into()
            .unwrap();
        self.file.seek(SeekFrom::Start(location))?;
        self.file.write_u64::<LittleEndian>(offset)?;
        Ok(())
    }
}

unsafe impl<'a> Send for Writer<'a> {}
unsafe impl<'a> Sync for Writer<'a> {}
