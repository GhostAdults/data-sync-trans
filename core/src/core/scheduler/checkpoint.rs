use anyhow::Result;
use redb::{Database, TableDefinition};
use std::path::Path;

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("checkpoints");

/// CDC Checkpoint 持久化存储
pub struct CheckpointStore {
    db: Database,
}

impl CheckpointStore {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        let write_tx = db.begin_write()?;
        write_tx.open_table(TABLE)?;
        write_tx.commit()?;
        Ok(Self { db })
    }

    pub fn save(&self, job_id: &str, position: &[u8]) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(TABLE)?;
            table.insert(job_id, position)?;
        }
        write_tx.commit()?;
        Ok(())
    }

    pub fn load(&self, job_id: &str) -> Option<Vec<u8>> {
        let read_tx = self.db.begin_read().ok()?;
        let table = read_tx.open_table(TABLE).ok()?;
        let access = table.get(job_id).ok()??;
        Some(access.value().to_vec())
    }

    pub fn remove(&self, job_id: &str) -> Result<()> {
        let write_tx = self.db.begin_write()?;
        {
            let mut table = write_tx.open_table(TABLE)?;
            let _ = table.remove(job_id);
        }
        write_tx.commit()?;
        Ok(())
    }
}
