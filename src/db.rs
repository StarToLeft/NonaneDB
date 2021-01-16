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

use fs2::*;

pub mod bucket;
pub mod descriptor;

use self::bucket::{document::DocumentConvert, Bucket};
use descriptor::DBDescriptor;

// Statically compiled options
/// Extension used for buckets
static EXTENSION: &'static str = ".page";

pub struct Database<'a, 'b> {
    store_dir: &'b Path,                            // Directory to store buckets
    buckets: BTreeMap<&'a str, Bucket>, // BTree of in-use buckets
    descriptor: Option<DBDescriptor>,
}

impl<'a, 'b> Database<'a, 'b> {
    pub fn open(path: &'b str) -> Result<Database<'a, 'b>, Box<dyn std::error::Error>> {
        // Initialize database struct
        let mut db = Database {
            store_dir: &Path::new(path),
            buckets: BTreeMap::new(),
            descriptor: None,
        };

        println!("Opening database");

        // Create the database directory if it doesn't exist
        if !db.store_dir.is_dir() {
            db.create_head_dir()?;
            println!("Created head directory");

            // Create descriptor file and write to it
            let dynamic = DBDescriptor::dynamic();
            dynamic.save_to_path(&db.store_dir.join(&Path::new("database.desc")))?;
            println!("Saved to path");

            // Assign descriptor
            db.descriptor = Some(dynamic);
        } else {
            println!("Loading from path");
            db.descriptor = Some(DBDescriptor::load_from_path(
                &db.store_dir.join(&Path::new("database.desc")),
            )?);
        }

        println!("Finished loading database");

        Ok(db)
    }

    pub fn create_head_dir(&self) -> std::io::Result<()> {
        // Create directory to hold buckets and database information
        Ok(fs::create_dir(self.store_dir)?)
    }

    pub fn open_bucket(
        &mut self,
        name: &'a str,
        descriptor: Option<BucketDescription>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Try to load an already existing bucket
        let res = self.load_bucket(&name, descriptor.clone());
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
                    .insert(name, Bucket::new(pager, p, true, descriptor)?);
            }
        }

        Ok(())
    }

    fn load_bucket(
        &self,
        name: &str,
        descriptor: Option<BucketDescription>,
    ) -> Result<Bucket, Box<dyn std::error::Error>> {
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
        Ok(Bucket::new(file, p, false, descriptor)?)
    }

    /// Inserts a new key and value into a bucket
    pub fn insert<T: DocumentConvert>(
        &mut self,
        bucket: &str,
        key: isize,
        value: T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bucket = self.buckets.get_mut(bucket);
        let bucket = match bucket {
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

        // Current solution loops through all fields, won't be very effiecent with a big amount of fields
        // Todo: Fix, solution is very slow (maybe a hashmap?)
        // Todo: (maybe generics to match it? Might not be able to in current rust versions)
        {
            for f in document.get_fields().iter() {
                let mut found_f = false;
                let p = bucket.descriptor.as_ref().unwrap().pull();
                for is_f in p.as_ref().field_description.iter() {
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

            bucket.insert(&document)?;
        }

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
