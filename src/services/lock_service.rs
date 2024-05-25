use fs2::FileExt;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;

pub struct LockService {
    path: PathBuf,
}

impl LockService {
    pub fn new(path: PathBuf) -> Self {
        LockService { path }
    }

    pub fn acquire(&self, key: String) -> std::io::Result<()> {
        let path = self.path.join(key);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        file.lock_exclusive()?;
        Ok(())
    }

    pub fn release(&self, key: String) -> std::io::Result<()> {
        let path = self.path.join(key);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        file.unlock()?;
        Ok(())
    }

    // Function to return a map of keys to their lock status
    pub fn check_locks(&self) -> std::io::Result<HashMap<String, bool>> {
        let mut status = HashMap::new();
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let file_name = path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                let file = OpenOptions::new().read(true).write(true).open(&path)?;

                // Try to acquire a shared lock without blocking
                match file.try_lock_shared() {
                    Ok(_) => {
                        // If we can lock it, then it's not locked by another process
                        status.insert(file_name, false);
                        file.unlock()?; // Unlock immediately since we were just checking
                    }
                    Err(_) => {
                        // If we cannot lock it, it's already locked
                        status.insert(file_name, true);
                    }
                }
            }
        }
        Ok(status)
    }

    pub fn is_locked(&self, key: String) -> std::io::Result<bool> {
        let path = self.path.join(key);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        match file.try_lock_shared() {
            Ok(_) => {
                file.unlock()?;
                Ok(false)
            }
            Err(_) => Ok(true),
        }
    }
}
