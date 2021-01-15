use std::{
    convert::TryInto,
    error::Error,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

/// Extra head-room added on-top of the header size to allow for compatability with future versions
const HEADER_ROOM: usize = 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DBDescriptor {
    pub(crate) page_size: usize,
    pub(crate) header_size: usize,
}

impl DBDescriptor {
    /// Initializes a descriptor with a default page size of 8 KB
    pub fn default() -> DBDescriptor {
        let mut desc = DBDescriptor::new();
        desc.page_size = 8192;

        desc
    }

    /// Initializes a dynamic descriptor, which fetches page size from the system
    pub fn dynamic() -> DBDescriptor {
        let mut desc = DBDescriptor::new();
        desc.page_size = page_size::get();

        desc
    }

    fn new() -> DBDescriptor {
        DBDescriptor {
            page_size: 0,
            header_size: std::mem::size_of::<DBDescriptor>() + HEADER_ROOM,
        }
    }

    /// Serializes a descriptor into bytes
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }

    /// Deserializes a descriptor from bytes
    pub fn deserialize(data: &[u8]) -> Result<DBDescriptor, Box<dyn Error>> {
        Ok(bincode::deserialize(&data)?)
    }

    pub fn load_from_path(path: &Path) -> Result<DBDescriptor, Box<dyn Error>> {
        // Open descriptor file
        let mut file = OpenOptions::new().read(true).read(true).write(true).open(&path)?;

        // Seek and read description length
        file.seek(SeekFrom::Start(0))?;
        let length = file.read_u64::<LittleEndian>()?.try_into()?;

        // Initialize buffer for reading
        let mut buf: Vec<u8> = Vec::with_capacity(length);
        unsafe { buf.set_len(length) };

        file.seek(SeekFrom::Current(std::mem::size_of::<u64>().try_into()?))?;
        file.read_exact(&mut buf)?;

        let descriptor = DBDescriptor::deserialize(&buf)?;
        Ok(descriptor)
    }

    pub fn save_to_path(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new().create_new(true).read(true).write(true).open(&path)?;

        // Serialize the descriptor and write it
        let buf = self.serialize();
        file.seek(SeekFrom::Start(0))?;
        file.allocate(self.header_size.try_into().unwrap())?;
        file.write_u64::<LittleEndian>(buf.len() as u64)?;
        file.seek(SeekFrom::Current(
            std::mem::size_of::<u64>().try_into().unwrap(),
        ))?;
        file.write(buf.as_slice())?;

        Ok(())
    }
}
