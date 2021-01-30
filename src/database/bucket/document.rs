pub mod field;
use std::ffi::{CStr, CString};

use field::Field;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    fields: Vec<Field>,
}

impl Document {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn read_field(&self, key: &str) -> Option<&Field> {
        let key = CString::new(key).expect("Failed to parse to CStr for read_field within document");

        for f in self.fields.iter() {
            if f.get_key() == key.as_c_str() {
                return Some(f);
            }
        }

        None
    }

    pub fn get_fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(bincode::serialize(&self)?)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(bincode::deserialize(bytes)?)
    }
}

pub trait DocumentConvert {
    type ConvertFrom;

    fn convert_to(self) -> Option<Document>;
    fn convert_from(doc: &Document) -> Option<Self::ConvertFrom>;
}
