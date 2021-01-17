use std::sync::{Arc, atomic::AtomicBool};

#[derive(Clone, Debug)]
pub struct Writer<'a> {
    name: &'a str,
    will_write: Arc<AtomicBool>,
}

impl<'a> Writer<'a> {
    pub fn new(name: &'a str, will_write: Arc<AtomicBool>) -> Self {
        Self { name, will_write }
    }
}

unsafe impl<'a> Send for Writer<'a> {}
unsafe impl<'a> Sync for Writer<'a> {}