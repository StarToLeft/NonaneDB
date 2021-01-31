#[derive(Clone, Copy, Debug, PartialEq, Hash)]
pub struct BucketConfiguration {
    drive_type: DriveType,
}

impl BucketConfiguration {
    pub fn new(drive_type: DriveType) -> BucketConfiguration {
        BucketConfiguration { drive_type }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Hash)]
pub enum DriveType {
    HDD,
    SSD,
}
