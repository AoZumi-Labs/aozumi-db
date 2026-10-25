//! Database name → open database. Each database is one file in the
//! data directory, opened once and shared by every connection to it.

use std::collections::HashMap;
use std::path::PathBuf;

use tokio::sync::Mutex;

pub struct Registry {
    dir: PathBuf,
    open: Mutex<HashMap<String, turso::Database>>,
}

impl Registry {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir, open: Mutex::new(HashMap::new()) }
    }

    pub async fn open(&self, name: &str) -> anyhow::Result<turso::Database> {
        let mut open = self.open.lock().await;
        if let Some(db) = open.get(name) {
            return Ok(db.clone());
        }
        tokio::fs::create_dir_all(&self.dir).await?;
        let path = self.path(name);
        let db = turso::Builder::new_local(path.to_str().unwrap()).build().await?;
        open.insert(name.to_string(), db.clone());
        Ok(db)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.db"))
    }
}
