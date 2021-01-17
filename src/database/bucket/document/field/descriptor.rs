use std::ffi::CString;

use super::{Field, fieldtype::FieldType};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDescriptor {
    name: CString,
    field_type: FieldType,
}

impl FieldDescriptor {
    pub fn new(name: &str, field_type: FieldType) -> FieldDescriptor {
        FieldDescriptor {
            name: CString::new(name).expect("Couldn't parse name, bytes incorrect for FieldDescriptor"),
            field_type
        }
    }

    pub fn is_match(&self, field: &Field) -> bool {
        if field.name == self.name && field.field_type == self.field_type {
            true
        } else {
            false
        }
    }
}