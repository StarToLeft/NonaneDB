#![allow(non_snake_case)]
#[macro_use]
extern crate serde;

pub mod db;
pub mod utils;

use db::{
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
    println!("Starting test");
    let t = std::time::Instant::now();
    let mut db: Database = Database::open("./database")?;
    let el = t.elapsed();
    println!("Open database {:?}", el);

    let t = std::time::Instant::now();
    let desc = BucketDescription {
        field_description: vec![
            FieldDescriptor::new("first_name".into(), FieldType::Text),
            FieldDescriptor::new("last_name".into(), FieldType::Text),
            FieldDescriptor::new("email".into(), FieldType::Text),
        ],
    };
    db.open_bucket("accounts", Some(desc))?;
    let el = t.elapsed();
    println!("Open bucket {:?}", el);

    let t = std::time::Instant::now();
    db.insert::<Account>(
        "accounts",
        0,
        Account::new("Anton", "Hagsér", "anton.hagser@epsidel.se"),
    )?;
    let el = t.elapsed();
    println!("Insert into bucket {:?}", el);

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
