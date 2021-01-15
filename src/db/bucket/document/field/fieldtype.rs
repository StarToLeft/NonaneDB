use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Uuid = 0x0,
    Bytes = 0x1,
    Text = 0x2,
    Int8 = 0x3,
    Int16 = 0x4,
    Int32 = 0x5,
    Int64 = 0x6,
    UInt8 = 0x7,
    UInt16 = 0x8,
    UInt32 = 0x9,
    UInt64 = 0xA,
    Float32 = 0xB,
    Float64 = 0xC,
}

/// Implemented on data types to convert them to bytes
pub trait ConvertFieldType<'a, T> {
    type Output;

    fn get_size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn get_type(&self) -> FieldType;
    fn serialize(&self) -> Option<Vec<u8>>;
    fn deserialize(d: &'a Vec<u8>) -> Option<Self::Output>;
}

impl<'a> ConvertFieldType<'a, uuid::Uuid> for uuid::Uuid {
    type Output = uuid::Uuid;

    fn get_type(&self) -> FieldType {
        FieldType::Uuid
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.as_bytes().to_vec())
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match uuid::Uuid::from_slice(&d) {
            Ok(u) => Some(u),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, &'a [u8]> for &[u8] {
    type Output = &'a [u8];

    fn get_type(&self) -> FieldType {
        FieldType::Bytes
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_vec())
    }

    fn deserialize(d: &'a Vec<u8>) -> Option<Self::Output> {
        Some(&d)
    }
}

impl<'a> ConvertFieldType<'a, &Vec<u8>> for &Vec<u8> {
    type Output = &'a [u8];

    fn get_type(&self) -> FieldType {
        FieldType::Bytes
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_vec())
    }

    fn deserialize(d: &'a Vec<u8>) -> Option<Self::Output> {
        Some(&d)
    }
}

impl<'a> ConvertFieldType<'a, &'a str> for &str {
    type Output = &'a str;

    fn get_type(&self) -> FieldType {
        FieldType::Text
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.as_bytes().to_vec())
    }

    fn deserialize(d: &'a Vec<u8>) -> Option<Self::Output> {
        match std::str::from_utf8(&d) {
            Ok(s) => Some(s),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, String> for String {
    type Output = String;

    fn get_type(&self) -> FieldType {
        FieldType::Text
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.as_bytes().to_vec())
    }

    fn deserialize(d: &'a Vec<u8>) -> Option<Self::Output> {
        match std::str::from_utf8(&d) {
            Ok(s) => Some(s.to_string()),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, String> for &String {
    type Output = String;

    fn get_type(&self) -> FieldType {
        FieldType::Text
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.as_bytes().to_vec())
    }

    fn deserialize(d: &'a Vec<u8>) -> Option<Self::Output> {
        match std::str::from_utf8(&d) {
            Ok(s) => Some(s.to_string()),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for i8 {
    type Output = i8;

    fn get_type(&self) -> FieldType {
        FieldType::Int8
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_i8(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_i8() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for i16 {
    type Output = i16;

    fn get_type(&self) -> FieldType {
        FieldType::Int16
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_i16::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_i16::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for i32 {
    type Output = i32;

    fn get_type(&self) -> FieldType {
        FieldType::Int32
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_i32::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_i32::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for i64 {
    type Output = i64;

    fn get_type(&self) -> FieldType {
        FieldType::Int64
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_i64::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_i64::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for u8 {
    type Output = u8;

    fn get_type(&self) -> FieldType {
        FieldType::UInt8
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_u8(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_u8() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for u16 {
    type Output = u16;

    fn get_type(&self) -> FieldType {
        FieldType::UInt16
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_u16::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_u16::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for u32 {
    type Output = u32;

    fn get_type(&self) -> FieldType {
        FieldType::UInt32
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_u32::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_u32::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for u64 {
    type Output = u64;

    fn get_type(&self) -> FieldType {
        FieldType::UInt64
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_u64::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_u64::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for f32 {
    type Output = f32;

    fn get_type(&self) -> FieldType {
        FieldType::Float32
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_f32::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_f32::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}

impl<'a> ConvertFieldType<'a, Self> for f64 {
    type Output = f64;

    fn get_type(&self) -> FieldType {
        FieldType::Float64
    }

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        match buf.write_f64::<LittleEndian>(*self) {
            Ok(_) => Some(buf),
            Err(_) => None
        }
    }

    fn deserialize(d: &Vec<u8>) -> Option<Self::Output> {
        match d.as_slice().read_f64::<LittleEndian>() {
            Ok(int) => Some(int),
            Err(_) => None,
        }
    }
}