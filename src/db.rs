#![allow(unused_imports, unused_variables)]
use std::io::prelude::*;
use std::io::SeekFrom;
use std::{
    self,
    collections::BTreeMap,
    fs::File,
    io::{Error, ErrorKind},
    path::Path,
    sync::{Arc, Mutex},
    *,
};
use std::{convert::TryInto, io};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use fs2::*;

pub mod bucket;
pub mod descriptor;

use self::bucket::{document::DocumentConvert, Bucket};
use descriptor::DBDescriptor;

// Statically compiled options
/// Extension used for buckets
static EXTENSION: &'static str = ".page";

pub struct Database<'a> {
    store_dir: &'a Path,                            // Directory to store buckets
    buckets: BTreeMap<&'a str, Arc<Mutex<Bucket>>>, // BTree of in-use buckets
    descriptor: Option<DBDescriptor>,
}

impl<'a> Database<'a> {
    pub fn open(path: &'a str) -> Result<Database<'a>, Box<dyn std::error::Error>> {
        // Initialize database struct
        let mut db = Database {
            store_dir: &Path::new(path),
            buckets: BTreeMap::new(),
            descriptor: None,
        };

        // Create the database directory if it doesn't exist
        if !db.store_dir.is_dir() {
            db.create_head_dir()?;

            // Create descriptor file and write to it
            let dynamic = DBDescriptor::dynamic();
            dynamic.save_to_path(&db.store_dir.join(&Path::new("database.desc")))?;

            // Assign descriptor
            db.descriptor = Some(dynamic);
        } else {
            db.descriptor = Some(DBDescriptor::load_from_path(
                &db.store_dir.join(&Path::new("database.desc")),
            )?);
        }

        Ok(db)
    }

    pub fn create_head_dir(&self) -> std::io::Result<()> {
        // Create directory to hold buckets and database information
        Ok(fs::create_dir(self.store_dir)?)
    }

    pub fn open_bucket(&mut self, name: &'a str) -> std::io::Result<()> {
        // Try to load an already existing bucket
        let res = self.load_bucket(&name);
        match res {
            Ok(b) => {
                // Load an existing bucket if it exists
                self.buckets.insert(name, b);
            }
            Err(_) => {
                // Create a new bucket if it doesn't exist
                let p = self
                    .store_dir
                    .join(Path::new(&(name.to_owned() + EXTENSION)));
                let pager = File::create(&p)?;
                self.buckets.insert(name, Bucket::new(pager, p, true)?);
            }
        }

        Ok(())
    }

    fn load_bucket(&self, name: &str) -> std::io::Result<Arc<Mutex<Bucket>>> {
        // Check if the bucket exists
        let p = self
            .store_dir
            .join(Path::new(&(name.to_owned() + EXTENSION)));
        if !p.exists() {
            return Err(Error::new(ErrorKind::NotFound, "bucket was not found"));
        }

        Ok(Bucket::new(File::open(&p)?, p, false)?)
    }

    /// Inserts a new key and value into a bucket
    pub fn insert<T: DocumentConvert>(
        &mut self,
        bucket: &str,
        key: isize,
        value: T,
    ) -> std::io::Result<()> {
        let bucket = self.buckets.get_mut(bucket);
        let bucket = match bucket {
            Some(b) => b,
            None => return Err(Error::new(ErrorKind::NotFound, "bucket was not found")),
        };

        // Get a document from the value
        let document = value.convert_to();
        let document = match document {
            Some(d) => d,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "failed to convert to document",
                ))
            }
        };

        let x = T::convert_from(&document);

        Ok(())
    }

    pub fn find<T>(&self, bucket: &str, key: isize) -> std::io::Result<Vec<T>> {
        let bucket = self.buckets.get(bucket);
        let bucket = match bucket {
            Some(b) => b,
            None => return Err(Error::new(ErrorKind::NotFound, "bucket was not found")),
        };

        Ok(Vec::new())
    }

    pub fn drop<T>(&mut self, bucket: &str, key: isize) -> std::io::Result<Vec<T>> {
        let bucket = self.buckets.get_mut(bucket);
        let bucket = match bucket {
            Some(b) => b,
            None => return Err(Error::new(ErrorKind::NotFound, "bucket was not found")),
        };

        Ok(Vec::new())
    }
}
