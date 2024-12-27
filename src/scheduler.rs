use crate::{
    async_commit::AsyncCommit,
    hint::ParallelExecutionHints,
    storage::{CacheDB, CachedStorageData},
    tx_dependency::TxDependency,
    LocationAndType, MemoryEntry, ReadVersion, Task, TransactionResult, TransactionStatus, TxId,
    TxState, TxVersion, CONCURRENT_LEVEL,
};
use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use auto_impl::auto_impl;
use dashmap::DashMap;
use revm::{Evm, EvmBuilder};
use revm_primitives::{db::DatabaseRef, EVMError, Env, SpecId, TxEnv, TxKind};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};
use std::cmp::max;
use std::sync::RwLock;
use std::time::Instant;
use crate::queue::LockFreeQueue;

pub type MVMemory = DashMap<LocationAndType, BTreeMap<TxId, MemoryEntry>>;

#[auto_impl(&)]
pub trait RewardsAccumulator {
    fn accumulate(&self, from: Option<TxId>, to: TxId) -> Option<u128>;
}

pub struct Scheduler<DB, C>
where
    DB: DatabaseRef,
    C: AsyncCommit,
{
    spec_id: SpecId,
    env: Env,
    block_size: usize,
    txs: Arc<Vec<TxEnv>>,
    db: DB,
    commiter: Mutex<C>,
    cache: CachedStorageData,
    tx_states: Vec<RwLock<TxState>>,
    tx_results: Vec<Mutex<Option<TransactionResult<DB::Error>>>>,
    tx_dependency: Mutex<TxDependency>,

    mv_memory: MVMemory,

    finality_idx: AtomicUsize,
    validation_idx: AtomicUsize,
    execution_idx: AtomicUsize,

    abort: AtomicBool,
}

impl<DB, C> RewardsAccumulator for Scheduler<DB, C>
where
    DB: DatabaseRef,
    C: AsyncCommit,
{
    fn accumulate(&self, from: Option<TxId>, to: TxId) -> Option<u128> {
        // register miner accumulator
        if to > 0 {
            if self.tx_states[to - 1].read().unwrap().status != TransactionStatus::Finality {
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

impl<DB, C> Scheduler<DB, C>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Clone + Send + Sync,
    C: AsyncCommit + Send,
{
    pub fn new(
        spec_id: SpecId,
        env: Env,
        txs: Arc<Vec<TxEnv>>,
        db: DB,
        commiter: C,
        with_hints: bool,
    ) -> Self {
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
            commiter: Mutex::new(commiter),
            cache: CachedStorageData::new(),
            tx_states: (0..num_txs).map(|_| RwLock::new(TxState::default())).collect(),
            tx_results: (0..num_txs).map(|_| Mutex::new(None)).collect(),
            tx_dependency: Mutex::new(tx_dependency),
            mv_memory: MVMemory::new(),
            finality_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            execution_idx: AtomicUsize::new(0),
            abort: AtomicBool::new(false),
        }
    }

    fn async_commit(&self) {
        let mut num_commit = 0;
        while !self.abort.load(Ordering::Relaxed) && num_commit < self.block_size {
            let finality_idx= self.finality_idx.load(Ordering::Relaxed);
            if num_commit == finality_idx {
                thread::yield_now();
            } else {
                while num_commit < finality_idx {
                    let result = self.tx_results[num_commit]
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .execute_result
                        .clone();
                    let Ok(result) = result else { panic!("Commit error tx: {}", num_commit) };
                    self.commiter.lock().unwrap().commit(result, &self.cache);
                    // println!("commit tx: {}", num_commit);
                    num_commit += 1;
                }
            }
        }
        // println!("finish commit {} txs", self.block_size);
    }

    pub fn with_commiter<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&mut C) -> R,
    {
        let mut commiter = self.commiter.lock().unwrap();
        func(&mut *commiter)
    }

    pub fn parallel_execute(
        &self,
        concurrency_level: Option<usize>,
    ) -> Result<(), EVMError<DB::Error>> {
        let concurrency_level = concurrency_level.unwrap_or(*CONCURRENT_LEVEL);
        let commit_notifier = (Mutex::new(false), Condvar::new());
        let task_queue = LockFreeQueue::new(concurrency_level * 2);
        thread::scope(|scope| {
            scope.spawn(|| {
                self.async_commit();
            });
            scope.spawn(|| {
                self.assign_tasks(&task_queue);
            });
            for _ in 0..concurrency_level {
                scope.spawn(|| {
                    let mut cache_db = CacheDB::new(
                        self.env.block.coinbase,
                        &self.db,
                        &self.cache,
                        &self.mv_memory,
                        &self,
                    );
                    let mut evm = EvmBuilder::default()
                        .with_db(&mut cache_db)
                        .with_spec_id(self.spec_id.clone())
                        .with_env(Box::new(self.env.clone()))
                        .build();
                    while let Some(task) = self.next(&task_queue) {
                        match task {
                            Task::Execution(tx_version) => {
                                self.execute(&mut evm, tx_version);
                            }
                            Task::Validation(tx_version) => {
                                self.validate(tx_version);
                            }
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
        // println!("start to execute tx: {}, incarnation: {}", txid, incarnation);
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
                    let mut tx_state = self.tx_states[txid].write().unwrap();
                    if tx_state.incarnation != incarnation {
                        panic!("Inconsistent incarnation when execution");
                    }
                    tx_state.status = if conflict {
                        TransactionStatus::Conflict
                    } else {
                        TransactionStatus::Executed
                    };
                    // println!("finish execute tx: {}, incarnation: {}, status={:?}", txid, incarnation, tx_state.status);
                }

                if conflict {
                    if !rewards_accumulated {
                        // Add all previous transactions as dependencies if miner doesn't accumulate
                        // the rewards
                        let dep_txs = self.finality_idx.load(Ordering::Relaxed)..txid;
                        let mut tx_dependency = self.tx_dependency.lock().unwrap();
                        tx_dependency.add(txid, dep_txs);
                    } else {
                        let dep_txs = self.generate_dependent_txs(txid, &read_set);
                        let mut tx_dependency = self.tx_dependency.lock().unwrap();
                        tx_dependency.add(txid, dep_txs);
                    }
                } else {
                    self.tx_dependency.lock().unwrap().remove(txid);
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
                    let mut tx_state = self.tx_states[txid].write().unwrap();
                    if tx_state.incarnation != incarnation {
                        panic!("Inconsistent incarnation when execution");
                    }
                    tx_state.status = TransactionStatus::Conflict;
                }
                {
                    let dep_txs = self.generate_dependent_txs(txid, &read_set);
                    self.tx_dependency.lock().unwrap().add(txid, dep_txs);
                }
                *last_result =
                    Some(TransactionResult { read_set, write_set, execute_result: Err(e) });
                if finality_idx == txid {
                    self.abort.store(true, Ordering::Relaxed);
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
        // println!("start validate tx: {}, incarnation: {}", txid, incarnation);

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
            let mut tx_state = self.tx_states[txid].write().unwrap();
            if tx_state.incarnation != incarnation {
                panic!("Inconsistent incarnation when validating");
            }
            tx_state.status =
                if conflict { TransactionStatus::Conflict } else { TransactionStatus::Unconfirmed };
            // println!("finish validate tx: {}, incarnation: {}, status={:?}", txid, incarnation, tx_state.status);
        }

        if conflict {
            // mark write set as estimate
            self.mark_estimate(txid, incarnation, &result.write_set);
            // update dependency
            let dep_txs = self.generate_dependent_txs(txid, &result.read_set);
            self.tx_dependency
                .lock()
                .unwrap()
                .add(txid, dep_txs);
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

    fn assign_tasks(&self, task_queue: &LockFreeQueue<Task>) {
        let mut start = Instant::now();
        let capacity = task_queue.capacity();
        let threshold = capacity / 2;
        while self.finality_idx.load(Ordering::Relaxed) < self.block_size &&
            !self.abort.load(Ordering::Relaxed) {
            let remaining_tasks = task_queue.len();
            if remaining_tasks < threshold {
                let num_tasks = capacity - remaining_tasks;
                let mut batch_task = Vec::with_capacity(num_tasks);
                let mut finality_idx = self.finality_idx.load(Ordering::Acquire);
                let mut validation_idx = self.validation_idx.load(Ordering::Acquire);
                let mut execution_idx = self.execution_idx.load(Ordering::Acquire);
                // Confirm the finality and conflict status
                if finality_idx < validation_idx {
                    while finality_idx < validation_idx {
                        let tx_status =
                            self.tx_states[finality_idx].read().unwrap().status.clone();
                        match tx_status {
                            TransactionStatus::Unconfirmed => {
                                self.tx_states[finality_idx].write().unwrap().status = TransactionStatus::Finality;
                                // println!("finality tx: {}", finality_idx);
                                finality_idx += 1;
                            }
                            TransactionStatus::Conflict | TransactionStatus::Executed => {
                                // Subsequent transactions after conflict need to be revalidated
                                validation_idx = finality_idx;
                                // println!("change validation cursor tx: {}, status={:?}", finality_idx, tx_status);
                                break;
                            }
                            _ => {
                                // println!("break tx: {}, status={:?}", finality_idx, tx_status);
                                break;
                            }
                        }
                    }
                    self.finality_idx.store(finality_idx, Ordering::Release);
                }

                // Prior to submit validation task
                while batch_task.len() < num_tasks {
                    if validation_idx < execution_idx {
                        let mut tx = self.tx_states[validation_idx].write().unwrap();
                        if matches!(tx.status, TransactionStatus::Executed | TransactionStatus::Unconfirmed)
                        {
                            tx.status = TransactionStatus::Validating;
                            batch_task.push(Task::Validation(TxVersion::new(validation_idx, tx.incarnation)));
                            // println!("submit validation tx: {}", validation_idx);
                            validation_idx += 1;
                        } else {
                            // println!("skip submit validation tx: {}, status={:?}", validation_idx, tx.status);
                            break;
                        }
                    } else {
                        break;
                    }
                }
                self.validation_idx.store(validation_idx, Ordering::Release);

                // Submit execution task
                while batch_task.len() < num_tasks {
                    let mut tx_dependency = self.tx_dependency.lock().unwrap();
                    if let Some((tasks, continuous_id)) = tx_dependency.next(num_tasks - batch_task.len()) {
                        // println!("get execution task: {:?}", tasks);
                        for execute_id in tasks.into_iter() {
                            let mut tx = self.tx_states[execute_id].write().unwrap();
                            if !matches!(tx.status, TransactionStatus::Initial | TransactionStatus::Conflict) {
                                // println!("start skip submit execution tx: {}, status={:?}", execute_id, tx.status);
                                tx_dependency.remove(execute_id);
                                // println!("finish skip submit execution tx: {}, and remove", execute_id);
                                continue;
                            }
                            execution_idx = max(execution_idx, continuous_id);
                            // println!("submit execution tx: {}, continuous_id={}", execute_id, continuous_id);
                            tx.status = TransactionStatus::Executing;
                            tx.incarnation += 1;
                            batch_task.push(Task::Execution(TxVersion::new(execute_id, tx.incarnation)));
                        }
                    } else {
                        break;
                    }
                }
                self.execution_idx.store(execution_idx, Ordering::Release);

                if batch_task.is_empty() {
                    thread::yield_now();
                    if (Instant::now() - start).as_millis() > 8_000 {
                        start = Instant::now();
                        println!(
                            "stuck..., finality_idx: {}, validation_idx: {}, execution_idx: {}",
                            self.finality_idx.load(Ordering::Acquire),
                            self.validation_idx.load(Ordering::Acquire),
                            self.execution_idx.load(Ordering::Acquire)
                        );
                        let status: Vec<(TxId, TransactionStatus)> = self
                            .tx_states
                            .iter()
                            .map(|s| s.read().unwrap().status.clone())
                            .enumerate()
                            .collect();
                        println!("transaction status: {:?}", status);
                        self.tx_dependency.lock().unwrap().print();
                        println!("task queue size: {}", task_queue.len());
                    }
                } else {
                    // println!("assign tasks: {:?}", batch_task);
                    for task in batch_task.into_iter() {
                        task_queue.push(task).expect("Failed to assign task");
                    }
                }
            } else {
                thread::yield_now();
            }
        }
    }

    pub fn next(&self, task_queue: &LockFreeQueue<Task>) -> Option<Task> {
        while self.finality_idx.load(Ordering::Relaxed) < self.block_size &&
            !self.abort.load(Ordering::Relaxed)
        {
            if let Some(task) = task_queue.pop() {
                // println!("next task: {:?}", task);
                return Some(task);
            } else {
                thread::yield_now();
            }
        }
        // println!("finish submit next task");
        None
    }
}
