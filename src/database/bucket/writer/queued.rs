use std::{sync::Arc, thread::JoinHandle};

use crossbeam_queue::ArrayQueue;

// Information about the writer thread
#[derive(Debug, Clone)]
pub struct WriterThread {
    pub(crate) join_handle: Arc<JoinHandle<QueuedWriter<'static>>>,
}

/// Data used to describe where the data will be written to
pub struct QueuedWriteInformation<'a> {
    seek: u64,
    len: usize,
    bytes: &'a [u8],
}

pub struct QueuedWriter<'a> {
    q: ArrayQueue<QueuedWriteInformation<'a>>,
}

impl<'a> QueuedWriter<'a> {
    pub fn new(q: ArrayQueue<QueuedWriteInformation>) -> QueuedWriter {
        QueuedWriter { q }
    }
}
