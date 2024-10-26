use std::{
    collections::{BTreeSet, HashMap, HashSet},
    ops::DerefMut,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use crate::{
    fork_join_util,
    hint::ParallelExecutionHints,
    partition::PartitionExecutor,
    storage::{LazyUpdateValue, SchedulerDB},
    tx_dependency::{DependentTxsVec, TxDependency},
    GrevmError, LocationAndType, ResultAndTransition, TransactionStatus, TxId, CPU_CORES,
    GREVM_RUNTIME, MAX_NUM_ROUND,
};

use metrics::{counter, gauge};
use revm::{
    db::{states::bundle_state::BundleRetention, BundleState},
    primitives::{
        AccountInfo, Address, Bytecode, EVMError, Env, ExecutionResult, SpecId, TxEnv, B256, U256,
    },
    CacheState, DatabaseRef, EvmBuilder,
};
use tracing::info;

struct ExecuteMetrics {
    /// Number of times parallel execution is called.
    parallel_execute_calls: metrics::Counter,
    /// Number of times sequential execution is called.
    sequential_execute_calls: metrics::Counter,

    /// Total number of transactions.
    total_tx_cnt: metrics::Counter,
    /// Number of transactions executed in parallel.
    parallel_tx_cnt: metrics::Counter,
    /// Number of transactions executed sequentially.
    sequential_tx_cnt: metrics::Counter,
    /// Number of transactions that encountered conflicts.
    conflict_tx_cnt: metrics::Counter,
    /// Number of transactions that reached finality.
    finality_tx_cnt: metrics::Counter,
    /// Number of transactions that are unconfirmed.
    unconfirmed_tx_cnt: metrics::Counter,
    /// Number of reusable transactions.
    reusable_tx_cnt: metrics::Counter,
    /// Number of transactions that skip validation
    skip_validation_cnt: metrics::Counter,

    /// Number of concurrent partitions.
    concurrent_partition_num: metrics::Gauge,
    /// Execution time difference between partitions(in nanoseconds).
    partition_et_diff: metrics::Gauge,
    /// Number of transactions difference between partitions.
    partition_tx_diff: metrics::Gauge,

    /// Time taken to parse execution hints(in nanoseconds).
    parse_hints_time: metrics::Counter,
    /// Time taken to partition transactions(in nanoseconds).
    partition_tx_time: metrics::Counter,
    /// Time taken to execute transactions in parallel(in nanoseconds).
    parallel_execute_time: metrics::Counter,
    /// Time taken to validate transactions(in nanoseconds).
    validate_time: metrics::Counter,
    /// Time taken to merge write set.
    merge_write_set_time: metrics::Counter,
    /// Time taken to commit transition
    commit_transition_time: metrics::Counter,
    /// Time taken to execute transactions in sequential(in nanoseconds).
    sequential_execute_time: metrics::Counter,
    /// Time taken to build output(in nanoseconds).
    build_output_time: metrics::Counter,
}

impl Default for ExecuteMetrics {
    fn default() -> Self {
        Self {
            parallel_execute_calls: counter!("grevm.parallel_round_calls"),
            sequential_execute_calls: counter!("grevm.sequential_execute_calls"),
            total_tx_cnt: counter!("grevm.total_tx_cnt"),
            parallel_tx_cnt: counter!("grevm.parallel_tx_cnt"),
            sequential_tx_cnt: counter!("grevm.sequential_tx_cnt"),
            finality_tx_cnt: counter!("grevm.finality_tx_cnt"),
            conflict_tx_cnt: counter!("grevm.conflict_tx_cnt"),
            unconfirmed_tx_cnt: counter!("grevm.unconfirmed_tx_cnt"),
            reusable_tx_cnt: counter!("grevm.reusable_tx_cnt"),
            skip_validation_cnt: counter!("grevm.skip_validation_cnt"),
            concurrent_partition_num: gauge!("grevm.concurrent_partition_num"),
            partition_et_diff: gauge!("grevm.partition_execution_time_diff"),
            partition_tx_diff: gauge!("grevm.partition_num_tx_diff"),
            parse_hints_time: counter!("grevm.parse_hints_time"),
            partition_tx_time: counter!("grevm.partition_tx_time"),
            parallel_execute_time: counter!("grevm.parallel_execute_time"),
            validate_time: counter!("grevm.validate_time"),
            merge_write_set_time: counter!("grevm.merge_write_set_time"),
            commit_transition_time: counter!("grevm.commit_transition_time"),
            sequential_execute_time: counter!("grevm.sequential_execute_time"),
            build_output_time: counter!("grevm.build_output_time"),
        }
    }
}

/// The output of the execution of a block.
#[derive(Debug)]
pub struct ExecuteOutput {
    /// The changed state of the block after execution.
    pub state: BundleState,
    /// All the results of the transactions in the block.
    pub results: Vec<ExecutionResult>,
}

/// A set of locations. Used to store the read and write sets of a transaction.
pub(crate) type LocationSet = HashSet<LocationAndType>;

/// The state of a transaction.
/// Contains the read and write sets of the transaction, as well as the result of executing the
/// transaction.
#[derive(Clone)]
pub(crate) struct TxState {
    pub tx_status: TransactionStatus,
    pub read_set: HashMap<LocationAndType, Option<U256>>,
    pub write_set: LocationSet,
    pub execute_result: ResultAndTransition,
}

impl TxState {
    pub(crate) fn new() -> Self {
        Self {
            tx_status: TransactionStatus::Initial,
            read_set: HashMap::new(),
            write_set: HashSet::new(),
            execute_result: ResultAndTransition::default(),
        }
    }
}

/// A shared reference to a vector of transaction states.
/// Used to share the transaction states between the partition executors.
/// Since the state of a transaction is not modified by multiple threads simultaneously,
/// `SharedTxStates` is thread-safe. Unsafe code is used to convert `SharedTxStates` to
/// a mutable reference, allowing modification of transaction states during execution.
pub(crate) type SharedTxStates = Arc<Vec<TxState>>;

/// The scheduler for executing transactions in parallel.
/// It partitions transactions into multiple groups and processes each group concurrently.
/// After each round of execution, it validates the transactions and updates their states.
/// The scheduler continues parallel execution until all transactions reach finality.
/// It falls back to sequential execution if not finished after a certain number of rounds.
#[allow(missing_debug_implementations)]
pub struct GrevmScheduler<DB>
where
    DB: DatabaseRef,
{
    spec_id: SpecId,
    env: Env,
    coinbase: Address,
    txs: Arc<Vec<TxEnv>>,

    /// The database utilized by the scheduler.
    /// It is shared among the partition executors,
    /// allowing them to read the final state from previous rounds.
    database: Arc<SchedulerDB<DB>>,

    /// The dependency relationship between transactions.
    /// Used to construct the next round of transaction partitions.
    tx_dependencies: TxDependency,

    /// Shared state of transactions.
    tx_states: SharedTxStates,

    /// number of partitions. maybe larger in the first round to increase concurrence
    num_partitions: usize,
    /// assigned tx IDs for each partition
    partitioned_txs: Vec<Vec<TxId>>,
    /// PartitionExecutors for each assigned tx IDs
    partition_executors: Vec<Arc<RwLock<PartitionExecutor<DB>>>>,
    /// number of finality txs in the current round
    num_finality_txs: usize,
    results: Vec<ExecutionResult>,

    metrics: ExecuteMetrics,
}

/// A wrapper for DatabaseRef.
/// Used to bypass the 'static constraint of `tokio::spawn`.
#[allow(missing_debug_implementations)]
pub struct DatabaseWrapper<Error>(Arc<dyn DatabaseRef<Error = Error> + Send + Sync + 'static>);

impl<Error> DatabaseRef for DatabaseWrapper<Error> {
    type Error = Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.0.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.0.code_by_hash_ref(code_hash)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.0.storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.0.block_hash_ref(number)
    }
}

/// Creates a new GrevmScheduler instance.
pub fn new_grevm_scheduler<DB>(
    spec_id: SpecId,
    env: Env,
    db: DB,
    txs: Vec<TxEnv>,
) -> GrevmScheduler<DatabaseWrapper<DB::Error>>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Clone + Send + Sync + 'static,
{
    // FIXME(gravity_nekomoto): use unsafe to bypass the 'static constraint of `tokio::spawn`.
    // We can be confident that the spawned task will be joined before the members in `db` are
    // dropped.
    let db = unsafe {
        let boxed: Arc<dyn DatabaseRef<Error = DB::Error>> = Arc::new(db);
        std::mem::transmute::<
            Arc<dyn DatabaseRef<Error = DB::Error>>,
            Arc<dyn DatabaseRef<Error = DB::Error> + Send + Sync + 'static>,
        >(boxed)
    };
    let db: DatabaseWrapper<DB::Error> = DatabaseWrapper(db);
    GrevmScheduler::new(spec_id, env, db, Arc::new(txs))
}

impl<DB> GrevmScheduler<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    /// Creates a new GrevmScheduler instance.
    pub fn new(spec_id: SpecId, env: Env, db: DB, txs: Arc<Vec<TxEnv>>) -> Self {
        let coinbase = env.block.coinbase;
        let num_partitions = *CPU_CORES * 2 + 1; // 2 * cpu + 1 for initial partition number
        let num_txs = txs.len();
        info!("Parallel execute {} txs of SpecId {:?}", num_txs, spec_id);
        Self {
            spec_id,
            env,
            coinbase,
            txs,
            database: Arc::new(SchedulerDB::new(db)),
            tx_dependencies: TxDependency::new(num_txs),
            tx_states: Arc::new(vec![TxState::new(); num_txs]),
            num_partitions,
            partitioned_txs: vec![],
            partition_executors: vec![],
            num_finality_txs: 0,
            results: Vec::with_capacity(num_txs),
            metrics: Default::default(),
        }
    }

    /// Get the partitioned transactions by dependencies.
    #[fastrace::trace]
    pub(crate) fn partition_transactions(&mut self) {
        // compute and assign partitioned_txs
        let start = Instant::now();
        self.partitioned_txs = self.tx_dependencies.fetch_best_partitions(self.num_partitions);
        self.num_partitions = self.partitioned_txs.len();
        let mut max = 0;
        let mut min = self.txs.len();
        for partition in &self.partitioned_txs {
            if partition.len() > max {
                max = partition.len();
            }
            if partition.len() < min {
                min = partition.len();
            }
        }
        self.metrics.partition_tx_diff.set((max - min) as f64);
        self.metrics.concurrent_partition_num.set(self.num_partitions as f64);
        self.metrics.partition_tx_time.increment(start.elapsed().as_nanos() as u64);
    }

    /// Execute transactions in parallel.
    #[fastrace::trace]
    fn round_execute(&mut self) -> Result<(), GrevmError<DB::Error>> {
        self.metrics.parallel_execute_calls.increment(1);
        self.partition_executors.clear();
        for partition_id in 0..self.num_partitions {
            let executor = PartitionExecutor::new(
                self.spec_id,
                partition_id,
                self.env.clone(),
                self.database.clone(),
                self.txs.clone(),
                self.tx_states.clone(),
                self.partitioned_txs[partition_id].clone(),
            );
            self.partition_executors.push(Arc::new(RwLock::new(executor)));
        }

        let start = Instant::now();
        GREVM_RUNTIME.block_on(async {
            let mut tasks = vec![];
            for executor in &self.partition_executors {
                let executor = executor.clone();
                tasks.push(GREVM_RUNTIME.spawn(async move { executor.write().unwrap().execute() }));
            }
            futures::future::join_all(tasks).await;
        });
        self.metrics.parallel_execute_time.increment(start.elapsed().as_nanos() as u64);

        self.validate_transactions()
    }

    /// Merge write set after each round
    #[fastrace::trace]
    fn merge_write_set(&mut self) -> (TxId, HashMap<LocationAndType, BTreeSet<TxId>>) {
        let start = Instant::now();
        let mut merged_write_set: HashMap<LocationAndType, BTreeSet<TxId>> = HashMap::new();
        let mut end_skip_id = self.num_finality_txs;
        for txid in self.num_finality_txs..self.tx_states.len() {
            if self.tx_states[txid].tx_status == TransactionStatus::SkipValidation &&
                end_skip_id == txid
            {
                end_skip_id += 1;
            } else {
                break;
            }
        }
        if end_skip_id != self.tx_states.len() {
            for txid in self.num_finality_txs..self.tx_states.len() {
                let tx_state = &self.tx_states[txid];
                for location in tx_state.write_set.iter() {
                    merged_write_set.entry(location.clone()).or_default().insert(txid);
                }
            }
        }
        self.metrics.merge_write_set_time.increment(start.elapsed().as_nanos() as u64);
        (end_skip_id, merged_write_set)
    }

    /// When validating the transaction status, the dependency relationship was updated.
    /// But there are some transactions that have entered the finality state,
    /// and there is no need to record the dependency and dependent relationships of these
    /// transactions. Thus achieving the purpose of pruning.
    #[fastrace::trace]
    fn update_and_pruning_dependency(&mut self) {
        let num_finality_txs = self.num_finality_txs;
        if num_finality_txs == self.txs.len() {
            return;
        }
        let mut new_dependency: Vec<DependentTxsVec> =
            vec![DependentTxsVec::new(); self.txs.len() - num_finality_txs];
        for executor in &self.partition_executors {
            let executor = executor.read().unwrap();
            for (txid, dep) in executor.assigned_txs.iter().zip(executor.tx_dependency.iter()) {
                if *txid >= num_finality_txs {
                    // pruning the tx that is finality state
                    new_dependency[*txid - num_finality_txs] = dep
                        .clone()
                        .into_iter()
                        // pruning the dependent tx that is finality state
                        .filter(|dep_id| *dep_id >= num_finality_txs)
                        .collect();
                }
            }
        }
        self.tx_dependencies.update_tx_dependency(new_dependency, num_finality_txs);
    }

    /// Generate unconfirmed transactions, and find the continuous minimum TxID,
    /// which can be marked as finality transactions.
    #[fastrace::trace]
    fn generate_unconfirmed_txs(&mut self) {
        let num_partitions = self.num_partitions;
        let (end_skip_id, merged_write_set) = self.merge_write_set();
        self.metrics.skip_validation_cnt.increment((end_skip_id - self.num_finality_txs) as u64);
        fork_join_util(num_partitions, Some(num_partitions), |_, _, part| {
            // Transaction validation process:
            // 1. For each transaction in each partition, traverse its read set and find the largest
            //    TxID(previous_txid) in merged_write_set that are less than the current
            //    transaction's TxId.
            // 2. A conflict occurs if: a) `previous_txid`` does not belong to the current
            //    transaction's partition, or b) `previous_txid` in same partition that is already
            //    marked as conflicted.
            let mut executor = self.partition_executors[part].write().unwrap();
            let executor = executor.deref_mut();

            #[allow(invalid_reference_casting)]
            let tx_states =
                unsafe { &mut *(&(*self.tx_states) as *const Vec<TxState> as *mut Vec<TxState>) };

            for txid in executor.assigned_txs.iter() {
                let txid = *txid;
                let mut conflict = tx_states[txid].tx_status == TransactionStatus::Conflict;
                let mut updated_dependencies = BTreeSet::new();
                if txid >= end_skip_id {
                    for (location, _) in tx_states[txid].read_set.iter() {
                        if let Some(written_txs) = merged_write_set.get(location) {
                            if let Some(previous_txid) = written_txs.range(..txid).next_back() {
                                // update dependencies: previous_txid <- txid
                                updated_dependencies.insert(*previous_txid);
                                if !conflict &&
                                    (!executor.assigned_txs.binary_search(previous_txid).is_ok() ||
                                        tx_states[*previous_txid].tx_status ==
                                            TransactionStatus::Conflict)
                                {
                                    conflict = true;
                                }
                            }
                        }
                    }
                }
                executor.tx_dependency.push(updated_dependencies);
                tx_states[txid].tx_status = if conflict {
                    TransactionStatus::Conflict
                } else {
                    TransactionStatus::Unconfirmed
                }
            }
        });
    }

    /// Find the continuous minimum TxID, which can be marked as finality transactions.
    /// If the smallest TxID is a conflict transaction, return an error.
    #[fastrace::trace]
    fn find_continuous_min_txid(&mut self) -> Result<usize, GrevmError<DB::Error>> {
        let mut min_execute_time = Duration::from_secs(u64::MAX);
        let mut max_execute_time = Duration::from_secs(0);
        for executor in &self.partition_executors {
            let mut executor = executor.write().unwrap();
            self.metrics.reusable_tx_cnt.increment(executor.metrics.reusable_tx_cnt);
            min_execute_time = min_execute_time.min(executor.metrics.execute_time);
            max_execute_time = max_execute_time.max(executor.metrics.execute_time);
            if executor.assigned_txs[0] == self.num_finality_txs &&
                self.tx_states[self.num_finality_txs].tx_status == TransactionStatus::Conflict
            {
                return Err(GrevmError::EvmError(
                    executor.error_txs.remove(&self.num_finality_txs).unwrap(),
                ));
            }
        }
        let mut conflict_tx_cnt = 0;
        let mut unconfirmed_tx_cnt = 0;
        let mut finality_tx_cnt = 0;
        self.metrics.partition_et_diff.set((max_execute_time - min_execute_time).as_nanos() as f64);
        #[allow(invalid_reference_casting)]
        let tx_states =
            unsafe { &mut *(&(*self.tx_states) as *const Vec<TxState> as *mut Vec<TxState>) };
        for txid in self.num_finality_txs..tx_states.len() {
            let tx_state = &mut tx_states[txid];
            match tx_state.tx_status {
                TransactionStatus::Unconfirmed => {
                    if txid == self.num_finality_txs {
                        self.num_finality_txs += 1;
                        finality_tx_cnt += 1;
                        tx_state.tx_status = TransactionStatus::Finality;
                    } else {
                        unconfirmed_tx_cnt += 1;
                    }
                }
                TransactionStatus::Conflict => {
                    conflict_tx_cnt += 1;
                }
                _ => {
                    return Err(GrevmError::UnreachableError(String::from(
                        "Wrong transaction status",
                    )));
                }
            }
        }
        self.metrics.conflict_tx_cnt.increment(conflict_tx_cnt as u64);
        self.metrics.unconfirmed_tx_cnt.increment(unconfirmed_tx_cnt as u64);
        self.metrics.finality_tx_cnt.increment(finality_tx_cnt as u64);
        info!(
            "Find continuous finality txs: conflict({}), unconfirmed({}), finality({})",
            conflict_tx_cnt, unconfirmed_tx_cnt, finality_tx_cnt
        );
        return Ok(finality_tx_cnt);
    }

    /// Commit the transition of the finality transactions, and update the minner's rewards.
    #[fastrace::trace]
    fn commit_transition(&mut self, finality_tx_cnt: usize) -> Result<(), GrevmError<DB::Error>> {
        let start = Instant::now();
        let partition_state: Vec<CacheState> = self
            .partition_executors
            .iter()
            .map(|executor| {
                let mut executor = executor.write().unwrap();
                std::mem::take(&mut executor.partition_db.cache)
            })
            .collect();

        // MUST drop the `PartitionExecutor::scheduler_db` before get mut
        self.partition_executors.clear();
        let database = Arc::get_mut(&mut self.database).unwrap();
        if self.num_finality_txs < self.txs.len() {
            // Merging these states is only useful when there is a next round of execution.
            Self::merge_not_modified_state(&mut database.cache, partition_state);
        }

        #[allow(invalid_reference_casting)]
        let tx_states =
            unsafe { &mut *(&(*self.tx_states) as *const Vec<TxState> as *mut Vec<TxState>) };
        let mut miner_updates = Vec::with_capacity(finality_tx_cnt);
        let start_txid = self.num_finality_txs - finality_tx_cnt;
        for txid in start_txid..self.num_finality_txs {
            miner_updates.push(tx_states[txid].execute_result.miner_update.clone());
            database
                .commit_transition(std::mem::take(&mut tx_states[txid].execute_result.transition));
            self.results.push(tx_states[txid].execute_result.result.clone().unwrap());
        }
        // Each transaction updates three accounts: from, to, and coinbase.
        // If every tx updates the coinbase account, it will cause conflicts across all txs.
        // Therefore, we handle miner rewards separately. We don't record miner’s address in r/w
        // set, and track the rewards for the miner for each transaction separately.
        // The miner’s account is only updated after validation by SchedulerDB.increment_balances
        database
            .update_balances(vec![(self.coinbase, LazyUpdateValue::merge(miner_updates))])
            .map_err(|err| GrevmError::EvmError(EVMError::Database(err)))?;
        self.metrics.commit_transition_time.increment(start.elapsed().as_nanos() as u64);
        Ok(())
    }

    /// verification of transaction state after each round
    /// Because after each round execution, the read-write set is no longer updated.
    /// We can check in parallel whether the read set is out of bounds.
    #[fastrace::trace]
    fn validate_transactions(&mut self) -> Result<(), GrevmError<DB::Error>> {
        let start = Instant::now();
        self.generate_unconfirmed_txs();
        let finality_tx_cnt = self.find_continuous_min_txid()?;
        // update and pruning tx dependencies
        self.update_and_pruning_dependency();
        self.commit_transition(finality_tx_cnt)?;
        self.metrics.validate_time.increment(start.elapsed().as_nanos() as u64);
        Ok(())
    }

    /// Merge not modified state from partition to scheduler. These data are just loaded from
    /// database, so we can merge them to state as original value for next round.
    #[fastrace::trace]
    fn merge_not_modified_state(state: &mut CacheState, partition_state: Vec<CacheState>) {
        for partition in partition_state {
            // merge account state that is not modified
            for (address, account) in partition.accounts {
                if account.status.is_not_modified() && state.accounts.get(&address).is_none() {
                    state.accounts.insert(address, account);
                }
            }

            // merge contract code
            for (hash, code) in partition.contracts {
                if state.contracts.get(&hash).is_none() {
                    state.contracts.insert(hash, code);
                }
            }
        }
    }

    /// Fall back to sequential execution for the remaining transactions.
    #[fastrace::trace]
    fn execute_remaining_sequential(&mut self) -> Result<(), GrevmError<DB::Error>> {
        let start = Instant::now();
        self.metrics.sequential_execute_calls.increment(1);
        self.metrics.sequential_tx_cnt.increment((self.txs.len() - self.num_finality_txs) as u64);
        // MUST drop the `PartitionExecutor::scheduler_db` before get mut
        self.partition_executors.clear();
        let database = Arc::get_mut(&mut self.database).unwrap();
        let mut evm = EvmBuilder::default()
            .with_db(database)
            .with_spec_id(self.spec_id)
            .with_env(Box::new(self.env.clone()))
            .build();
        for txid in self.num_finality_txs..self.txs.len() {
            if let Some(tx) = self.txs.get(txid) {
                *evm.tx_mut() = tx.clone();
            } else {
                return Err(GrevmError::UnreachableError(String::from("Wrong transactions ID")));
            }
            match evm.transact() {
                Ok(result_and_state) => {
                    evm.db_mut().commit(result_and_state.state);
                    self.results.push(result_and_state.result);
                }
                Err(err) => return Err(GrevmError::EvmError(err)),
            }
        }
        self.metrics.sequential_execute_time.increment(start.elapsed().as_nanos() as u64);
        Ok(())
    }

    #[fastrace::trace]
    fn build_output(&mut self) -> ExecuteOutput {
        let start = Instant::now();
        // MUST drop the `PartitionExecutor::scheduler_db` before get mut
        self.partition_executors.clear();
        let database = Arc::get_mut(&mut self.database).unwrap();
        database.merge_transitions(BundleRetention::Reverts);
        let output = ExecuteOutput {
            state: std::mem::take(&mut database.bundle_state),
            results: std::mem::take(&mut self.results),
        };
        self.metrics.build_output_time.increment(start.elapsed().as_nanos() as u64);
        output
    }

    #[fastrace::trace]
    fn parse_hints(&mut self) {
        let start = Instant::now();
        let hints = ParallelExecutionHints::new(self.tx_states.clone());
        hints.parse_hints(self.txs.clone());
        self.tx_dependencies.init_tx_dependency(self.tx_states.clone());
        self.metrics.parse_hints_time.increment(start.elapsed().as_nanos() as u64);
    }

    #[fastrace::trace]
    fn evm_execute(
        &mut self,
        force_sequential: Option<bool>,
        with_hints: bool,
        num_partitions: Option<usize>,
    ) -> Result<ExecuteOutput, GrevmError<DB::Error>> {
        if with_hints {
            self.parse_hints();
        }
        if let Some(num_partitions) = num_partitions {
            self.num_partitions = num_partitions;
        }

        self.metrics.total_tx_cnt.increment(self.txs.len() as u64);
        let force_parallel = !force_sequential.unwrap_or(true); // adaptive false
        let force_sequential = force_sequential.unwrap_or(false); // adaptive false

        if self.txs.len() < self.num_partitions && !force_parallel {
            self.execute_remaining_sequential()?;
            return Ok(self.build_output());
        }

        if !force_sequential {
            let mut round = 0;
            while round < MAX_NUM_ROUND {
                if self.num_finality_txs < self.txs.len() {
                    self.partition_transactions();
                    if self.num_partitions == 1 && !force_parallel {
                        break;
                    }
                    round += 1;
                    self.round_execute()?;
                } else {
                    break;
                }
            }
            self.metrics.parallel_tx_cnt.increment(self.num_finality_txs as u64);
        }

        if self.num_finality_txs < self.txs.len() {
            info!("Sequential execute {} remaining txs", self.txs.len() - self.num_finality_txs);
            self.execute_remaining_sequential()?;
        }

        Ok(self.build_output())
    }

    /// Execute transactions in parallel.
    pub fn parallel_execute(mut self) -> Result<ExecuteOutput, GrevmError<DB::Error>> {
        self.evm_execute(None, true, None)
    }

    /// Execute transactions parallelly with or without hints.
    pub fn force_parallel_execute(
        mut self,
        with_hints: bool,
        num_partitions: Option<usize>,
    ) -> Result<ExecuteOutput, GrevmError<DB::Error>> {
        self.evm_execute(Some(false), with_hints, num_partitions)
    }

    /// Execute transactions sequentially.
    pub fn force_sequential_execute(mut self) -> Result<ExecuteOutput, GrevmError<DB::Error>> {
        self.evm_execute(Some(true), false, None)
    }
}
