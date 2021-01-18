use parking_lot::{Mutex, Condvar};
use std::{sync::Arc, time::Duration};

#[derive(Debug)]
pub struct BooleanSemaphore {
    mutex: Arc<Mutex<bool>>,
    cvar: Condvar,
}

impl BooleanSemaphore {
    pub fn new (value: Arc<Mutex<bool>>) -> Self {
        BooleanSemaphore {
            mutex: value,
            cvar: Condvar::new(),
        }
    }

    pub fn wait(&self) {
        let mut value = self.mutex.lock();
        while !(*value) {
            self.cvar.wait(&mut value);
        }
    }

    pub fn wait_for(&self, timeout: Duration) {
        let mut value = self.mutex.lock();
        while !(*value) {
            let _ = self.cvar.wait_for(&mut value, timeout);
        }
    }

    pub fn set_ready(&self, ready: bool) {
        let mut value = self.mutex.lock();
        *value = ready;
        self.cvar.notify_all();
    }
}