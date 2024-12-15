use crate::{scheduler::MVMemory, LocationAndType, ReadVersion};
use ahash::AHashMap as HashMap;
use revm_primitives::{
    db::{Database, DatabaseRef},
    AccountInfo, Address, Bytecode, B256, U256,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct CacheState<DB>
where
    DB: DatabaseRef,
{
    db: DB,
    mv_memory: Arc<MVMemory>,
    accounts: BTreeMap<Address, Result<Option<AccountInfo>, DB::Error>>,
    bytecodes: BTreeMap<Address, Result<Bytecode, DB::Error>>,
    slots: BTreeMap<(Address, U256), Result<U256, DB::Error>>,
    block_hashes: BTreeMap<u64, Result<B256, DB::Error>>,
}

impl<DB> CacheState<DB>
where
    DB: DatabaseRef,
{
    pub fn new(db: DB, mv_memory: Arc<MVMemory>) -> Self {
        Self {
            db,
            mv_memory,
            accounts: BTreeMap::new(),
            bytecodes: BTreeMap::new(),
            slots: BTreeMap::new(),
            block_hashes: BTreeMap::new(),
        }
    }

    pub fn miner_involved(&self) -> bool {
        todo!()
    }

    pub fn visit_estimate(&self) -> bool {
        todo!()
    }

    pub fn take_read_set(&self) -> HashMap<LocationAndType, ReadVersion> {
        todo!()
    }
}

impl<DB> DatabaseRef for CacheState<DB>
where
    DB: DatabaseRef,
{
    type Error = DB::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        todo!()
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        todo!()
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        todo!()
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}

impl<DB> Database for CacheState<DB>
where
    DB: DatabaseRef,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        todo!()
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        todo!()
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        todo!()
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        todo!()
    }
}
