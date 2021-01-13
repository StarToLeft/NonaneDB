#[macro_use]
extern crate serde;

pub mod db;

use db::{
    bucket::document::{field::Field, Document, DocumentConvert},
    Database,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t = std::time::Instant::now();
    let mut db: Database = Database::open("./database")?;
    let el = t.elapsed();
    println!("Open database {:?}", el);

    let t = std::time::Instant::now();
    db.open_bucket("accounts")?;
    let el = t.elapsed();
    println!("Open bucket {:?}", el);

    db.insert::<Account>(
        "accounts",
        0,
        Account::new("Anton", "Hagsér", "anton.hagser@epsidel.se"),
    )?;

    Ok(())
}

pub struct Account {
    first_name: String,
    last_name: String,
    email: String,
    data: Vec<u8>,
}

impl Account {
    pub fn new(first_name: &str, last_name: &str, email: &str) -> Account {
        let mut data: Vec<u8> = Vec::with_capacity(1_048_576);
        unsafe { data.set_len(1_048_576) };

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

    fn convert_to(&self) -> Option<Document> {
        let first_name = Field::new("first_name", self.first_name.to_owned())?;
        let last_name = Field::new("last_name", self.last_name.to_string())?;
        let email = Field::new("email", self.email.to_string())?;

        let data = Field::new("data", &self.data)?;

        Some(Document::new(vec![first_name, last_name, email, data]))
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
