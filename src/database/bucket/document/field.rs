pub mod descriptor;
pub mod fieldtype;

use std::{ffi::{CStr, CString}, sync::{Arc, Mutex}};

use fieldtype::{ConvertFieldType, FieldType};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    name: CString,
    field_type: FieldType,
    data: Vec<u8>,
}

impl<'a> Field {
    pub fn new<T: ConvertFieldType<'a, T>>(name: &'a str, data: T) -> Option<Field> {
        let field_type = data.get_type();
        let data = data.serialize();

        match data {
            Some(data) => Some(Self {
                name: CString::new(name).expect("Failed to parse Field name to CString, bytes incorrect"),
                field_type,
                data,
            }),
            None => None,
        }
    }

    pub fn new_bytes(name: &'a str, mut bytes: &mut Vec<u8>) -> Option<Field> {
        let mut data = Vec::new();
        std::mem::swap(&mut data, &mut bytes);

        Some(Self {
            name: CString::new(name).expect("Failed to parse Field name to CString, bytes incorrect"),
            field_type: FieldType::Bytes,
            data,
        })
    }

    pub fn get_key(&self) -> &CStr {
        self.name.as_c_str()
    }

    pub fn get_value<T: ConvertFieldType<'a, T>>(
        &'a self,
    ) -> Option<<T as ConvertFieldType<'a, T>>::Output> {
        T::deserialize(&self.data)
    }

    pub fn get_type(&self) -> &FieldType {
        &self.field_type
    }
}
