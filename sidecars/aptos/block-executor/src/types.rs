use aptos_types::transaction::Transaction;
use serde::{Deserialize, Serialize};
use aptos_crypto::HashValue;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub ts : u64,
    pub id : HashValue, 
    pub transactions : Vec<Transaction>,
}

impl Block {

    pub fn hash_value(&self) -> Result<HashValue, anyhow::Error> {
        Ok(self.id.clone())
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use aptos_crypto::HashValue;

    #[test]
    fn test_block_hash_value() -> Result<(), anyhow::Error> {
        let block = Block {
            ts : 0, 
            id : HashValue::random(),
            transactions : vec![],
        };

        let hash_value = block.hash_value()?;

        Ok(())

    }

}