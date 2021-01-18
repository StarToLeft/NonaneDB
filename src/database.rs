#![allow(unused_imports, unused_variables)]
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
use std::{fs::OpenOptions, io::prelude::*};

use bucket::descriptor::BucketDescription;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use dashmap::{DashMap, mapref::one::RefMut};
use fs2::*;
use log::trace;

pub mod bucket;
pub mod descriptor;

use self::bucket::{document::DocumentConvert, Bucket};
use descriptor::DBDescriptor;

// Statically compiled options
/// Extension used for buckets
static EXTENSION: &'static str = ".page";

#[derive(Clone)]
pub struct Database<'a, 'b> {
    store_dir: Arc<&'b Path>,              // Directory to store buckets
    buckets: DashMap<&'a str, Bucket<'a>>, // BTree of in-use buckets
    descriptor: Arc<Option<DBDescriptor>>,
}

impl<'a, 'b> Database<'a, 'b> {
    pub fn open(path: &'b str) -> Result<Database<'a, 'b>, Box<dyn std::error::Error>> {
        // Initialize database struct
        let mut db = Database {
            store_dir: Arc::new(&Path::new(path)),
            buckets: DashMap::new(),
            descriptor: Arc::new(None),
        };

        // Create the database directory if it doesn't exist
        trace!("Checking if database already exists");
        if !db.store_dir.is_dir() {
            db.create_head_dir()?;

            // Create descriptor file and write to it
            let dynamic = DBDescriptor::dynamic();
            dynamic.save_to_path(&db.store_dir.join(&Path::new("database.desc")))?;

            // Assign descriptor
            db.descriptor = Arc::new(Some(dynamic));
        } else {
            db.descriptor = Arc::new(Some(DBDescriptor::load_from_path(
                &db.store_dir.join(&Path::new("database.desc")),
            )?));
        }

        trace!("Successfully loaded and initialized a database");
        Ok(db)
    }

    /// Creates directory to hold buckets and database information
    pub fn create_head_dir(&self) -> std::io::Result<()> {
        trace!("Creating head directory for database");
        Ok(fs::create_dir(self.store_dir.as_ref())?)
    }

    pub fn open_bucket(
        &mut self,
        name: &'a str,
        descriptor: Option<BucketDescription>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Try to load an already existing bucket
        let res = self.load_bucket(name.clone(), descriptor.clone());
        match res {
            Ok(b) => {
                // Load an existing bucket if it exists
                self.buckets.insert(name, b);
            }
            Err(e) => {
                // Create a new bucket if it doesn't exist
                let p = self
                    .store_dir
                    .join(Path::new(&(name.to_owned() + EXTENSION)));
                let pager = File::create(&p)?;
                let pager = OpenOptions::new().read(true).write(true).open(&p)?;
                self.buckets
                    .insert(name, Bucket::new(name, pager, p, true, descriptor)?);
            }
        }

        Ok(())
    }

    fn load_bucket(
        &self,
        name: &'a str,
        descriptor: Option<BucketDescription>,
    ) -> Result<Bucket<'a>, Box<dyn std::error::Error>> {
        // Check if the bucket exists
        let p = self
            .store_dir
            .join(Path::new(&(name.to_owned() + EXTENSION)));
        if !p.exists() {
            return Err(Box::new(Error::new(
                ErrorKind::NotFound,
                "bucket was not found",
            )));
        }

        let file = OpenOptions::new().read(true).write(true).open(&p)?;
        Ok(Bucket::new(name, file, p, false, descriptor)?)
    }

    /// Try to fetch a mutable reference to an internal bucket
    pub fn borrow_buckets(&mut self, bucket: &'a str) -> DashMap<&'a str, Bucket<'a>> {
        self.buckets.clone()
    }

    /// Inserts a new key and value into a bucket
    pub fn insert<T: DocumentConvert>(
        &mut self,
        bucket: &str,
        key: isize,
        value: T,
    ) -> Result<(usize, [u8; 24]), Box<dyn std::error::Error>> {
        let bucket = self.buckets.get_mut(bucket);
        let mut bucket = match bucket {
            Some(b) => b,
            None => {
                return Err(Box::new(Error::new(
                    ErrorKind::NotFound,
                    "bucket was not found",
                )))
            }
        };

        // Get a document from the value
        let document = value.convert_to();
        let document = match document {
            Some(d) => d,
            None => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "failed to convert to document",
                )))
            }
        };

        {
            let p = bucket.descriptor.as_ref().as_ref().unwrap().pull();
            let p = p.as_ref();
            let field_description = &p.field_description;

            if document.get_fields().len() < field_description.len() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "to few fields where defined in insert request",
                )));
            } else if document.get_fields().len() > field_description.len() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "to many fields where defined in insert request",
                )));
            }

            // Current solution loops through all fields, won't be very effiecent with a big amount of fields
            // Todo: Fix, solution is very slow (maybe a hashmap?)
            // Todo: (maybe generics to match it? Might not be able to in current rust versions)
            for f in document.get_fields().iter() {
                let mut found_f = false;
                for is_f in field_description.iter() {
                    if is_f.is_match(f) {
                        found_f = true;
                    }
                }

                if !found_f {
                    return Err(Box::new(Error::new(
                        ErrorKind::NotFound,
                        "field does not exist",
                    )));
                }
            }
        }

        Ok(bucket.insert(&document)?)
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
