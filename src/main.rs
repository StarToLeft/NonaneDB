#![allow(non_snake_case)]
#[macro_use]
extern crate serde;

#[macro_use]
extern crate log;

use rayon::prelude::*;

pub mod database;
pub mod utils;

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
        ],
    };
    db.open_bucket("accounts", Some(desc.clone()))?;
    let el = t.elapsed();
    debug!("It took {:?} to initialize 'accounts' bucket", el);

    (0..4).into_par_iter().for_each(|_| {
        let mut database = db.clone();
        let elements = 10;
        let t = std::time::Instant::now();
        for _ in 0..elements {
            let _ = database.insert::<Account>(
                "accounts",
                0,
                Account::new("Anton", "Hagsér", "anton.hagser@epsidel.se"),
            );
        }
        let el = t.elapsed();
        info!(
            "Time taken for each of {} elements: {:?}, time for all: {:?}",
            elements,
            el / elements,
            el
        );
    });

    std::thread::sleep(std::time::Duration::from_secs(40));
    Ok(())
}

pub struct Account {
    first_name: String,
    last_name: String,
    email: String,
}

impl Account {
    pub fn new(first_name: &str, last_name: &str, email: &str) -> Account {
        Account {
            first_name: first_name.to_string(),
            last_name: last_name.to_string(),
            email: email.to_string(),
        }
    }
}

impl DocumentConvert for Account {
    type ConvertFrom = Account;

    fn convert_to(&self) -> Option<Document> {
        let first_name = Field::new("first_name", self.first_name.to_owned())?;
        let last_name = Field::new("last_name", self.last_name.to_string())?;
        let email = Field::new("email", self.email.to_string())?;

        Some(Document::new(vec![first_name, last_name, email]))
    }

    fn convert_from(doc: &Document) -> Option<Self::ConvertFrom> {
        let first_name = doc.read_field("first_name")?;
        let last_name = doc.read_field("last_name")?;
        let email = doc.read_field("email")?;

        Some(Account::new(
            first_name.get_value::<&str>()?,
            last_name.get_value::<&str>()?,
            email.get_value::<&str>()?,
        ))
    }
}
