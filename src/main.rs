#![allow(non_snake_case)]
#[macro_use]
extern crate serde;

#[macro_use]
extern crate log;

pub mod database;
pub mod utils;

use std::{sync::atomic::Ordering, time::Duration};

use database::{
    bucket::{
        descriptor::BucketDescription,
        document::{
            field::{descriptor::FieldDescriptor, fieldtype::FieldType, Field},
            Document, DocumentConvert,
        },
    },
    Database,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Starting database and initializing buckets");
    let t = std::time::Instant::now();
    let mut db: Database = Database::open("./database")?;
    let el = t.elapsed();
    debug!("It took {:?} to start the database", el);

    info!("Initializing bucket");
    let t = std::time::Instant::now();
    let desc = BucketDescription {
        field_description: vec![
            FieldDescriptor::new("first_name".into(), FieldType::Text),
            FieldDescriptor::new("last_name".into(), FieldType::Text),
            FieldDescriptor::new("email".into(), FieldType::Text),
            FieldDescriptor::new("data".into(), FieldType::Bytes),
        ],
    };

    db.open_bucket("accounts", Some(desc.clone()))?;

    let el = t.elapsed();
    debug!("It took {:?} to initialize 'accounts' bucket", el);

    let mut dbe = db.clone();
    let mut buck = dbe.get_mut_bucket("accounts")?;
    let count = buck.count_documents()?;

    info!(
        "Initially counted {} documents in bucket {}",
        count,
        buck.get_name()
    );

    std::thread::sleep(Duration::from_millis(1000));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(16)
        .build()
        .unwrap();

    let installations = 100;

    let insert_time = std::time::Instant::now();
    for _ in 0..installations {
        let database = db.clone();
        pool.install(move || insert(database, 10000));
    }

    // Wait for thread writing to finish
    {
        let mut dbx = db.clone();
        let bucks = dbx.borrow_buckets();
        let buck = bucks.get_mut("accounts");
        let buck = buck.as_ref();
        let buck = buck.unwrap();
        let writer_thread = buck.writer_thread.clone().unwrap();

        while writer_thread.items.load(Ordering::SeqCst) > 0 {}
    }

    let el = insert_time.elapsed();

    // Count the new documents
    let mut buck = db.get_mut_bucket("accounts")?;
    let c = buck.count_documents()?;

    info!(
        "Counted {} documents in bucket {}, Last count: {}",
        c,
        buck.get_name(),
        count
    );

    info!(
        "Test: Insert time for {} elements was counted to: {:?}",
        c - count,
        el
    );

    Ok(())
}

pub fn insert(mut database: Database, el: i32) {
    let mut data = Vec::new();
    for _ in 0..1_048_576 {
        data.push(0);
    }

    for _ in 0..el {
        let d = data.clone();
        let r = database.insert::<Account>(
            "accounts",
            0,
            Account::new("Anton", "Hagsér", "anton.hagser@epsidel.se", d),
        );

        if r.is_err() {
            debug!("{:?}", r);
        }
    }
}

pub struct Account {
    first_name: String,
    last_name: String,
    email: String,
    data: Vec<u8>,
}

impl Account {
    pub fn new(first_name: &str, last_name: &str, email: &str, data: Vec<u8>) -> Account {
        Account {
            first_name: first_name.to_string(),
            last_name: last_name.to_string(),
            email: email.to_string(),
            data,
        }
    }
}

impl DocumentConvert for Account {
    type ConvertFrom = Account;

    fn convert_to(self) -> Option<Document> {
        let first_name = Field::new("first_name", self.first_name.to_owned())?;
        let last_name = Field::new("last_name", self.last_name.to_string())?;
        let email = Field::new("email", self.email.to_string())?;
        let data = Field::new("data", self.data.as_slice())?;

        Some(Document::new(vec![first_name, last_name, email, data]))
    }

    fn convert_from(doc: &Document) -> Option<Self::ConvertFrom> {
        let first_name = doc.read_field("first_name")?;
        let last_name = doc.read_field("last_name")?;
        let email = doc.read_field("email")?;
        let data = doc.read_field("data")?;

        Some(Account::new(
            first_name.get_value::<&str>()?,
            last_name.get_value::<&str>()?,
            email.get_value::<&str>()?,
            Vec::from(data.get_value::<&[u8]>()?),
        ))
    }
}
