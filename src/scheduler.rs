use crate::{
    hint::ParallelExecutionHints,
    storage::{CacheDB, SharedCacheData},
    tx_dependency::TxDependency,
    LocationAndType, MemoryEntry, ReadVersion, Task, TransactionResult, TransactionStatus, TxId,
    TxState, TxVersion, CONCURRENT_LEVEL,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use auto_impl::auto_impl;
use dashmap::DashMap;
use revm::{Evm, EvmBuilder};
use revm_primitives::{db::DatabaseRef, EVMError, EVMResult, Env, SpecId, TxEnv, TxKind};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

pub type MVMemory = DashMap<LocationAndType, BTreeMap<TxId, MemoryEntry>>;

#[auto_impl(&)]
pub trait RewardsAccumulator {
    fn accumulate(&self, from: Option<TxId>, to: TxId) -> Option<u128>;
}

pub struct Scheduler<DB>
where
    DB: DatabaseRef,
{
    spec_id: SpecId,
    env: Env,
    block_size: usize,
    txs: Arc<Vec<TxEnv>>,
    db: DB,
    tx_states: Vec<Mutex<TxState>>,
    tx_results: Vec<Mutex<Option<TransactionResult<DB::Error>>>>,
    tx_dependency: Mutex<TxDependency>,

    mv_memory: MVMemory,

    finality_idx: AtomicUsize,
    validation_idx: AtomicUsize,
    execution_idx: AtomicUsize,

    abort: AtomicBool,
}

impl<DB> RewardsAccumulator for Scheduler<DB>
where
    DB: DatabaseRef,
{
    fn accumulate(&self, from: Option<TxId>, to: TxId) -> Option<u128> {
        // register miner accumulator
        if to > 0 {
            if self.tx_states[to - 1].lock().unwrap().status != TransactionStatus::Finality {
                return None;
            }
        }
        let mut rewards = 0;
        for prev in from.unwrap_or(0)..to {
            let tx_result = self.tx_results[prev].lock().unwrap();
            let Some(result) = tx_result.as_ref() else {
                return None;
            };
            let Ok(result) = &result.execute_result else {
                return None;
            };
            rewards += result.rewards;
        }
        Some(rewards)
    }
}

impl<DB> Scheduler<DB>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Clone + Send + Sync,
{
    pub fn new(spec_id: SpecId, env: Env, txs: Arc<Vec<TxEnv>>, db: DB, with_hints: bool) -> Self {
        let num_txs = txs.len();
        let tx_dependency = if with_hints {
            ParallelExecutionHints::new(txs.clone()).parse_hints()
        } else {
            TxDependency::new(num_txs)
        };
        Self {
            spec_id,
            env,
            block_size: num_txs,
            txs,
            db,
            tx_states: (0..num_txs).map(|_| Mutex::new(TxState::default())).collect(),
            tx_results: (0..num_txs).map(|_| Mutex::new(None)).collect(),
            tx_dependency: Mutex::new(tx_dependency),
            mv_memory: MVMemory::new(),
            finality_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            execution_idx: AtomicUsize::new(0),
            abort: AtomicBool::new(false),
        }
    }

    pub fn parallel_execute(
        &self,
        concurrency_level: Option<usize>,
    ) -> Result<(), EVMError<DB::Error>> {
        let concurrency_level = concurrency_level.unwrap_or(*CONCURRENT_LEVEL);
        let cache = SharedCacheData::new();
        let coinbase = self.env.block.coinbase.clone();
        let lock = Mutex::new(());
        thread::scope(|scope| {
            for _ in 0..concurrency_level {
                scope.spawn(|| {
                    let mut cache_db = CacheDB::new(
                        self.env.block.coinbase,
                        &self.db,
                        &cache,
                        &self.mv_memory,
                        &self,
                    );
                    let mut evm = EvmBuilder::default()
                        .with_db(&mut cache_db)
                        .with_spec_id(self.spec_id.clone())
                        .with_env(Box::new(self.env.clone()))
                        .build();
                    loop {
                        let task = {
                            let _lock = lock.lock().unwrap();
                            self.next()
                        };
                        if let Some(task) = task {
                            match task {
                                Task::Execution(tx_version) => {
                                    self.execute(&mut evm, tx_version);
                                }
                                Task::Validation(tx_version) => {
                                    self.validate(tx_version);
                                }
                            }
                        } else {
                            break;
                        }
                    }
                });
            }
        });
        if self.abort.load(Ordering::Relaxed) {
            let result = self.tx_results[self.finality_idx.load(Ordering::Relaxed)].lock().unwrap();
            if let Some(result) = result.as_ref() {
                if let Err(e) = &result.execute_result {
                    return Err(e.clone());
                }
            }
            panic!("Wrong abort transaction")
        } else {
            Ok(())
        }
    }

    fn execute<RA>(&self, evm: &mut Evm<'_, (), &mut CacheDB<DB, RA>>, tx_version: TxVersion)
    where
        RA: RewardsAccumulator,
    {
        let finality_idx = self.finality_idx.load(Ordering::Relaxed);
        let TxVersion { txid, incarnation } = tx_version;
        evm.db_mut().reset_state(TxVersion::new(txid, incarnation));
        *evm.tx_mut() = self.txs[txid].clone();
        let result = evm.transact_lazy_reward();

        match result {
            Ok(result_and_state) => {
                // only the miner involved in transaction should accumulate the rewards of finality
                // txs return true if the tx doesn't visit the miner account
                let rewards_accumulated = evm.db().rewards_accumulated();
                let conflict = !rewards_accumulated || evm.db().visit_estimate();
                let read_set = evm.db_mut().take_read_set();
                let write_set = evm.db().update_mv_memory(&result_and_state.state, conflict);

                let mut last_result = self.tx_results[txid].lock().unwrap();
                if let Some(last_result) = last_result.as_ref() {
                    for location in &last_result.write_set {
                        if !write_set.contains(location) {
                            if let Some(mut written_transactions) = self.mv_memory.get_mut(location)
                            {
                                written_transactions.remove(&txid);
                            }
                        }
                    }
                }

                // update transaction status
                {
                    let mut tx_state = self.tx_states[txid].lock().unwrap();
                    if tx_state.incarnation != incarnation {
                        panic!("Inconsistent incarnation when execution");
                    }
                    tx_state.status = if conflict {
                        TransactionStatus::Conflict
                    } else {
                        TransactionStatus::Executed
                    };
                }

                if conflict {
                    let mut tx_dependency = self.tx_dependency.lock().unwrap();
                    if !rewards_accumulated {
                        // Add all previous transactions as dependencies if miner doesn't accumulate
                        // the rewards
                        for dep_id in self.finality_idx.load(Ordering::Relaxed)..txid {
                            tx_dependency.update_dependency(txid, dep_id);
                        }
                    } else {
                        for dep_id in self.generate_dependent_txs(txid, &read_set) {
                            tx_dependency.update_dependency(txid, dep_id);
                        }
                    }
                }
                *last_result = Some(TransactionResult {
                    read_set,
                    write_set,
                    execute_result: Ok(result_and_state),
                });
            }
            Err(e) => {
                let mut read_set = evm.db_mut().take_read_set();
                let mut write_set = HashSet::new();
                read_set
                    .entry(LocationAndType::Basic(evm.tx().caller))
                    .or_insert(ReadVersion::Storage);
                if let TxKind::Call(to) = evm.tx().transact_to {
                    read_set.entry(LocationAndType::Basic(to)).or_insert(ReadVersion::Storage);
                }

                let mut last_result = self.tx_results[txid].lock().unwrap();
                if let Some(last_result) = last_result.as_mut() {
                    write_set = std::mem::take(&mut last_result.write_set);
                    for location in write_set.iter() {
                        if let Some(mut written_transactions) = self.mv_memory.get_mut(location) {
                            if let Some(entry) = written_transactions.get_mut(&txid) {
                                entry.estimate = true;
                            }
                        }
                    }
                }

                // update transaction status
                {
                    let mut tx_state = self.tx_states[txid].lock().unwrap();
                    if tx_state.incarnation != incarnation {
                        panic!("Inconsistent incarnation when execution");
                    }
                    tx_state.status = TransactionStatus::Conflict;
                }
                let mut tx_dependency = self.tx_dependency.lock().unwrap();
                for dep_id in self.generate_dependent_txs(txid, &read_set) {
                    tx_dependency.update_dependency(txid, dep_id);
                }
                *last_result =
                    Some(TransactionResult { read_set, write_set, execute_result: Err(e) });
                if finality_idx == txid {
                    self.abort.store(true, Ordering::Release);
                }
            }
        }
    }

    fn validate(&self, tx_version: TxVersion) {
        let TxVersion { txid, incarnation } = tx_version;
        // check the read version of read set
        let mut conflict = false;
        let tx_result = self.tx_results[txid].lock().unwrap();
        let Some(result) = tx_result.as_ref() else {
            panic!("No result when validating");
        };
        if let Err(_) = &result.execute_result {
            panic!("Error transaction should take as conflict before validating");
        }

        for (location, version) in result.read_set.iter() {
            if let Some(written_transactions) = self.mv_memory.get(location) {
                if let Some((previous_id, latest_version)) =
                    written_transactions.range(..txid).next_back()
                {
                    if let ReadVersion::MvMemory(version) = version {
                        if version.txid != *previous_id ||
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

        // update transaction status
        {
            let mut tx_state = self.tx_states[txid].lock().unwrap();
            if tx_state.incarnation != incarnation {
                panic!("Inconsistent incarnation when validating");
            }
            tx_state.status =
                if conflict { TransactionStatus::Conflict } else { TransactionStatus::Unconfirmed };
        }

        if conflict {
            // update dependency
            let dep_ids = self.generate_dependent_txs(txid, &result.read_set);
            self.tx_dependency.lock().unwrap().add(txid, dep_ids);

            // mark write set as estimate
            self.mark_estimate(txid, incarnation, &result.write_set);
        }
    }

    fn mark_estimate(&self, txid: TxId, incarnation: usize, write_set: &HashSet<LocationAndType>) {
        for location in write_set {
            if let Some(mut written_transactions) = self.mv_memory.get_mut(location) {
                if let Some(entry) = written_transactions.get_mut(&txid) {
                    if entry.incarnation == incarnation {
                        entry.estimate = true;
                    }
                }
            }
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
                    let dep_id = *dep_id;
                    if dep_id > self.finality_idx.load(Ordering::Relaxed) {
                        dep_ids.insert(dep_id);
                    }
                }
            }
        }
        dep_ids
    }

    pub fn next(&self) -> Option<Task> {
        while self.finality_idx.load(Ordering::Relaxed) < self.block_size &&
            !self.abort.load(Ordering::Relaxed)
        {
            // Confirm the finality and conflict status
            while self.finality_idx.load(Ordering::Relaxed) <
                self.validation_idx.load(Ordering::Relaxed)
            {
                let mut tx =
                    self.tx_states[self.finality_idx.load(Ordering::Relaxed)].lock().unwrap();
                match tx.status {
                    TransactionStatus::Unconfirmed => {
                        tx.status = TransactionStatus::Finality;
                        let txid = self.finality_idx.fetch_add(1, Ordering::Release);
                        self.tx_dependency.lock().unwrap().remove(txid);
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
                    return Some(Task::Validation(TxVersion::new(validation_idx, tx.incarnation)));
                } else {
                    break;
                }
            }
            // Submit execution task
            let mut tx_dependency = self.tx_dependency.lock().unwrap();
            while let Some(execute_id) = tx_dependency.next() {
                let mut tx = self.tx_states[execute_id].lock().unwrap();
                if !matches!(tx.status, TransactionStatus::Initial | TransactionStatus::Conflict) {
                    tx_dependency.remove(execute_id);
                    continue;
                }
                self.execution_idx.fetch_max(execute_id, Ordering::Release);
                tx.status = TransactionStatus::Executing;
                tx.incarnation += 1;
                return Some(Task::Execution(TxVersion::new(execute_id, tx.incarnation)));
            }
            thread::yield_now();
        }
        None
    }
}
