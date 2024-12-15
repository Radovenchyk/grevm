use crate::{
    hint::ParallelExecutionHints, storage::CacheState, tx_dependency::TxDependency,
    LocationAndType, MemoryEntry, ReadVersion, Task, TransactionResult, TransactionStatus, TxId,
    TxState, CONCURRENT_LEVEL,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use dashmap::DashMap;
use revm::EvmBuilder;
use revm_primitives::{db::DatabaseRef, TxEnv};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

pub type MVMemory = DashMap<LocationAndType, BTreeMap<TxId, MemoryEntry>>;

pub struct Scheduler<DB>
where
    DB: DatabaseRef,
{
    concurrency_level: usize,
    block_size: usize,
    txs: Arc<Vec<TxEnv>>,
    cache_state: CacheState<DB>,
    tx_states: Vec<Mutex<TxState>>,
    tx_results: Vec<Mutex<TransactionResult<DB::Error>>>,
    tx_dependency: Mutex<TxDependency>,

    mv_memory: Arc<MVMemory>,

    finality_idx: AtomicUsize,
    validation_idx: AtomicUsize,
    execution_idx: AtomicUsize,
}

impl<DB> Scheduler<DB>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Send + Sync,
{
    pub fn new(concurrency_level: usize, txs: Arc<Vec<TxEnv>>, db: DB, with_hints: bool) -> Self {
        let mv_memory = Arc::new(MVMemory::new());
        let tx_dependency = if with_hints {
            ParallelExecutionHints::new(txs.clone()).parse_hints()
        } else {
            TxDependency::new(txs.len())
        };
        Self {
            concurrency_level,
            block_size: txs.len(),
            txs,
            cache_state: CacheState::new(db, mv_memory.clone()),
            tx_states: vec![],
            tx_results: vec![],
            tx_dependency: Mutex::new(tx_dependency),
            mv_memory,
            finality_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            execution_idx: AtomicUsize::new(0),
        }
    }

    pub fn parallel_evm(&self, concurrency_level: Option<usize>) {
        let concurrency_level = concurrency_level.unwrap_or(*CONCURRENT_LEVEL);
        let lock = Mutex::new(());
        thread::scope(|scope| {
            for _ in 0..self.concurrency_level {
                scope.spawn(|| loop {
                    let task = {
                        let _lock = lock.lock().unwrap();
                        self.next()
                    };
                    if let Some(task) = task {
                        match task {
                            Task::Execution(txid) => {
                                self.execute(txid);
                            }
                            Task::Validation(txid) => {
                                self.validate(txid);
                            }
                        }
                    } else {
                        break;
                    }
                });
            }
        });
    }

    fn execute(&self, txid: TxId) {
        let mut evm = EvmBuilder::default()
            .with_ref_db(&self.cache_state)
            // .with_spec_id(self.spec_id)
            // .with_env(Box::new(std::mem::take(&mut self.env)))
            .build();

        let result = evm.transact_lazy_reward();
        let miner_involved = self.cache_state.miner_involved();
        let conflict = self.cache_state.visit_estimate();
        let read_set = self.cache_state.take_read_set();

        if conflict {
            if miner_involved {
                let mut tx_dependency = self.tx_dependency.lock().unwrap();
                for dep_id in self.finality_idx.load(Ordering::Relaxed)..txid {
                    tx_dependency.update_dependency(txid, dep_id);
                }
            } else {
                let mut tx_dependency = self.tx_dependency.lock().unwrap();
                for dep_id in self.generate_dependent_txs(txid, &read_set) {
                    tx_dependency.update_dependency(txid, dep_id);
                }
            }
        } else {
            self.tx_dependency.lock().unwrap().remove(txid);
        }
    }

    fn validate(&self, txid: TxId) {
        // check the read version of read set
        let mut conflict = false;
        let tx_state = self.tx_results[txid].lock().unwrap();
        for (location, version) in tx_state.read_set.iter() {
            if let Some(written_transactions) = self.mv_memory.get(location) {
                if let Some((previous_tid, latest_version)) =
                    written_transactions.range(..txid).next_back()
                {
                    if let ReadVersion::MvMemory(version) = version {
                        let latest_version = &latest_version.tx_version;
                        if version.txid != latest_version.txid ||
                            version.incarnation != latest_version.incarnation
                        {
                            conflict = true;
                            break;
                        }
                    } else {
                        conflict = true;
                        break;
                    }
                } else if !matches!(version, ReadVersion::Storage) {
                    conflict = true;
                    break;
                }
            } else if !matches!(version, ReadVersion::Storage) {
                conflict = true;
                break;
            }
        }

        // update dependency
        if conflict {
            let dep_ids = self.generate_dependent_txs(txid, &tx_state.read_set);
            self.tx_dependency.lock().unwrap().add(txid, dep_ids);
        }

        // update transaction status
        {
            let mut tx_state = self.tx_states[txid].lock().unwrap();
            tx_state.status =
                if conflict { TransactionStatus::Conflict } else { TransactionStatus::Unconfirmed };
        }
    }

    fn generate_dependent_txs(
        &self,
        txid: TxId,
        read_set: &HashMap<LocationAndType, ReadVersion>,
    ) -> HashSet<TxId> {
        let mut dep_ids = HashSet::new();
        for location in read_set.keys() {
            if let Some(written_transactions) = self.mv_memory.get(location) {
                // To prevent dependency explosion, only add the tx with the highest TxId in
                // written_transactions
                if let Some((dep_id, _)) = written_transactions.range(..txid).next_back() {
                    dep_ids.insert(*dep_id);
                }
            }
        }
        dep_ids
    }

    pub fn next(&self) -> Option<Task> {
        while self.finality_idx.load(Ordering::Relaxed) < self.block_size {
            // Confirm the finality and conflict status
            while self.finality_idx.load(Ordering::Relaxed) <
                self.validation_idx.load(Ordering::Relaxed)
            {
                let mut tx =
                    self.tx_states[self.finality_idx.load(Ordering::Relaxed)].lock().unwrap();
                match tx.status {
                    TransactionStatus::Unconfirmed => {
                        tx.status = TransactionStatus::Finality;
                        self.finality_idx.fetch_add(1, Ordering::Release);
                    }
                    TransactionStatus::Conflict => {
                        // Subsequent transactions after conflict need to be revalidated
                        self.validation_idx
                            .store(self.finality_idx.load(Ordering::Relaxed), Ordering::Release);
                        break;
                    }
                    _ => {
                        break;
                    }
                }
            }
            // Prior to submit validation task
            while self.validation_idx.load(Ordering::Relaxed) <
                self.execution_idx.load(Ordering::Relaxed)
            {
                let validation_idx = self.validation_idx.load(Ordering::Relaxed);
                let mut tx = self.tx_states[validation_idx].lock().unwrap();
                if matches!(tx.status, TransactionStatus::Executed | TransactionStatus::Unconfirmed)
                {
                    self.validation_idx.fetch_add(1, Ordering::Release);
                    tx.status = TransactionStatus::Validating;
                    return Some(Task::Validation(validation_idx));
                }
            }
            // Submit execution task
            let execute_id = self.tx_dependency.lock().unwrap().next();
            if let Some(execute_id) = execute_id {
                let mut tx = self.tx_states[execute_id].lock().unwrap();
                if matches!(tx.status, TransactionStatus::Executing | TransactionStatus::Finality) {
                    panic!("Unreachable");
                }
                self.execution_idx.fetch_max(execute_id, Ordering::Release);
                tx.status = TransactionStatus::Executing;
                tx.incarnation += 1;
                return Some(Task::Execution(execute_id));
            } else {
                thread::yield_now();
            }
        }
        None
    }
}
