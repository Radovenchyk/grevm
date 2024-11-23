use crate::{fork_join_util, LocationAndType, LocationSet};
use ahash::{AHashMap, AHashSet};
use fastrace::Span;
use revm::{
    db::{
        states::{bundle_state::BundleRetention, CacheAccount},
        AccountRevert, BundleAccount, BundleState, PlainAccount,
    },
    precompile::Address,
    primitives::{Account, AccountInfo, Bytecode, EvmState, B256, BLOCK_HASH_HISTORY, U256},
    CacheState, Database, DatabaseCommit, DatabaseRef, TransitionAccount, TransitionState,
};
use std::{
    collections::{btree_map, hash_map, BTreeMap, HashMap},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

trait ParallelBundleState {
    fn parallel_apply_transitions_and_create_reverts(
        &mut self,
        transitions: TransitionState,
        retention: BundleRetention,
    );
}

impl ParallelBundleState for BundleState {
    #[fastrace::trace]
    fn parallel_apply_transitions_and_create_reverts(
        &mut self,
        transitions: TransitionState,
        retention: BundleRetention,
    ) {
        if !self.state.is_empty() {
            self.apply_transitions_and_create_reverts(transitions, retention);
            return;
        }

        let include_reverts = retention.includes_reverts();
        // pessimistically pre-allocate assuming _all_ accounts changed.
        let reverts_capacity = if include_reverts { transitions.transitions.len() } else { 0 };
        let transitions = transitions.transitions;
        let addresses: Vec<Address> = transitions.keys().cloned().collect();
        let reverts: Vec<Option<(Address, AccountRevert)>> = vec![None; reverts_capacity];
        let bundle_state: Vec<Option<(Address, BundleAccount)>> = vec![None; transitions.len()];
        let state_size = AtomicUsize::new(0);
        let contracts = Mutex::new(HashMap::new());

        let span = Span::enter_with_local_parent("parallel create reverts");
        fork_join_util(transitions.len(), None, |start_pos, end_pos, _| {
            #[allow(invalid_reference_casting)]
            let reverts = unsafe {
                &mut *(&reverts as *const Vec<Option<(Address, AccountRevert)>>
                    as *mut Vec<Option<(Address, AccountRevert)>>)
            };
            #[allow(invalid_reference_casting)]
            let addresses =
                unsafe { &mut *(&addresses as *const Vec<Address> as *mut Vec<Address>) };
            #[allow(invalid_reference_casting)]
            let bundle_state = unsafe {
                &mut *(&bundle_state as *const Vec<Option<(Address, BundleAccount)>>
                    as *mut Vec<Option<(Address, BundleAccount)>>)
            };

            for pos in start_pos..end_pos {
                let address = addresses[pos];
                let transition = transitions.get(&address).cloned().unwrap();
                // add new contract if it was created/changed.
                if let Some((hash, new_bytecode)) = transition.has_new_contract() {
                    contracts.lock().unwrap().insert(hash, new_bytecode.clone());
                }
                let present_bundle = transition.present_bundle_account();
                let revert = transition.create_revert();
                if let Some(revert) = revert {
                    state_size.fetch_add(present_bundle.size_hint(), Ordering::Relaxed);
                    bundle_state[pos] = Some((address, present_bundle));
                    if include_reverts {
                        reverts[pos] = Some((address, revert));
                    }
                }
            }
        });
        self.state_size = state_size.load(Ordering::Acquire);
        drop(span);

        // much faster than bundle_state.into_iter().filter_map(|r| r).collect()
        self.state.reserve(transitions.len());
        for bundle in bundle_state {
            if let Some((address, state)) = bundle {
                self.state.insert(address, state);
            }
        }
        let mut final_reverts = Vec::with_capacity(reverts_capacity);
        for revert in reverts {
            if let Some(r) = revert {
                final_reverts.push(r);
            }
        }
        self.reverts.push(final_reverts);
        self.contracts = contracts.into_inner().unwrap();
    }
}

/// State of blockchain.
///
/// State clear flag is set inside CacheState and by default it is enabled.
/// If you want to disable it use `set_state_clear_flag` function.
#[derive(Debug, Clone)]
pub struct State {
    /// Cache the committed data of finality txns and the read-only data during execution after
    /// each round of execution. Used as the initial state for the next round of partition
    /// executors. When fall back to sequential execution, used as cached state contains both
    /// changed from evm execution and cached/loaded account/storages.
    pub cache: CacheState,
    /// Block state, it aggregates transactions transitions into one state.
    ///
    /// Build reverts and state that gets applied to the state.
    // TODO(gravity_nekomoto): Try to directly generate bundle state from cache, rather than
    // transitions.
    pub transition_state: Option<TransitionState>,
    /// After block is finishes we merge those changes inside bundle.
    /// Bundle is used to update database and create changesets.
    /// Bundle state can be set on initialization if we want to use preloaded bundle.
    pub bundle_state: BundleState,
    /// If EVM asks for block hash we will first check if they are found here.
    /// and then ask the database.
    ///
    /// This map can be used to give different values for block hashes if in case
    /// The fork block is different or some blocks are not saved inside database.
    pub block_hashes: BTreeMap<u64, B256>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            // TODO(gravity): Set state clear flag if the block is after the Spurious Dragon
            // hardfork.
            cache: CacheState::default(),
            transition_state: Some(TransitionState::default()),
            bundle_state: BundleState::default(),
            block_hashes: BTreeMap::new(),
        }
    }
}

impl State {
    /// Takes the current bundle state.
    /// It is typically called after the bundle state has been finalized.
    pub fn take_bundle(&mut self) -> BundleState {
        std::mem::take(&mut self.bundle_state)
    }

    /// Take all transitions and merge them inside bundle state.
    /// This action will create final post state and all reverts so that
    /// we at any time revert state of bundle to the state before transition is applied.
    #[fastrace::trace]
    pub fn merge_transitions(&mut self, retention: BundleRetention) {
        if let Some(transition_state) = self.transition_state.as_mut().map(TransitionState::take) {
            self.bundle_state
                .parallel_apply_transitions_and_create_reverts(transition_state, retention);
        }
    }
}

/// SchedulerDB is a database wrapper that manages state transitions and caching for the EVM.
/// It maintains a cache of committed data, a transition state for ongoing transactions, and a
/// bundle state for finalizing block state changes. It also tracks block hashes for quick access.
///
/// After each execution round, SchedulerDB caches the committed data of finalized
/// transactions and the read-only data accessed during execution.
/// This cached data serves as the initial state for the next round of partition executors.
/// When reverting to sequential execution, these cached states will include both
/// the changes from EVM execution and the cached/loaded accounts and storages.
#[allow(missing_debug_implementations)]
pub struct SchedulerDB<DB> {
    /// The cached state during execution.
    pub state: Box<State>,

    /// The underlying database that stores the state.
    pub database: DB,
}

impl<DB> SchedulerDB<DB> {
    /// Create new SchedulerDB with database
    pub fn new(state: Box<State>, database: DB) -> Self {
        Self { state, database }
    }

    /// This function is used to cache the committed data of finality txns and the read-only data
    /// during execution. These data will be used as the initial state for the next round of
    /// partition executors. When falling back to sequential execution, these cached states will
    /// include both the changes from EVM execution and the cached/loaded accounts/storages.
    pub(crate) fn commit_transition(&mut self, transitions: Vec<(Address, TransitionAccount)>) {
        apply_transition_to_cache(&mut self.state.cache, &transitions);
        self.apply_transition(transitions);
    }

    /// Apply transition to transition state.
    /// This will be used to create final post state and reverts.
    fn apply_transition(&mut self, transitions: Vec<(Address, TransitionAccount)>) {
        // add transition to transition state.
        if let Some(s) = self.state.transition_state.as_mut() {
            s.add_transitions(transitions)
        }
    }
}

impl<DB> SchedulerDB<DB>
where
    DB: DatabaseRef,
{
    /// Load account from cache or database.
    /// If account is not found in cache, it will be loaded from database.
    fn load_cache_account(&mut self, address: Address) -> Result<&mut CacheAccount, DB::Error> {
        match self.state.cache.accounts.entry(address) {
            hash_map::Entry::Vacant(entry) => {
                let info = self.database.basic_ref(address)?;
                Ok(entry.insert(into_cache_account(info)))
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    /// The miner's rewards is calculated by subtracting the previous balance from the current
    /// balance. and should add to the miner's account after each round of execution for
    /// finality transactions.
    pub(crate) fn increment_balances(
        &mut self,
        balances: impl IntoIterator<Item = (Address, u128)>,
    ) -> Result<(), DB::Error> {
        // make transition and update cache state
        let mut transitions = Vec::new();
        for (address, balance) in balances {
            if balance == 0 {
                continue;
            }
            let original_account = self.load_cache_account(address)?;
            transitions.push((
                address,
                original_account.increment_balance(balance).expect("Balance is not zero"),
            ))
        }
        // append transition
        if let Some(s) = self.state.transition_state.as_mut() {
            s.add_transitions(transitions)
        }
        Ok(())
    }
}

fn into_cache_account(account: Option<AccountInfo>) -> CacheAccount {
    match account {
        None => CacheAccount::new_loaded_not_existing(),
        Some(acc) if acc.is_empty() => CacheAccount::new_loaded_empty_eip161(HashMap::new()),
        Some(acc) => CacheAccount::new_loaded(acc, HashMap::new()),
    }
}

/// Get storage value of address at index.
fn load_storage<DB: DatabaseRef>(
    cache: &mut CacheState,
    database: &DB,
    address: Address,
    index: U256,
) -> Result<U256, DB::Error> {
    // Account is guaranteed to be loaded.
    // Note that storage from bundle is already loaded with account.
    if let Some(account) = cache.accounts.get_mut(&address) {
        // account will always be some, but if it is not, U256::ZERO will be returned.
        let is_storage_known = account.status.is_storage_known();
        Ok(account
            .account
            .as_mut()
            .map(|account| match account.storage.entry(index) {
                hash_map::Entry::Occupied(entry) => Ok(*entry.get()),
                hash_map::Entry::Vacant(entry) => {
                    // if account was destroyed or account is newly built
                    // we return zero and don't ask database.
                    let value = if is_storage_known {
                        U256::ZERO
                    } else {
                        tokio::task::block_in_place(|| database.storage_ref(address, index))?
                    };
                    entry.insert(value);
                    Ok(value)
                }
            })
            .transpose()?
            .unwrap_or_default())
    } else {
        unreachable!("For accessing any storage account is guaranteed to be loaded beforehand")
    }
}

/// Apply transition to cache state.
fn apply_transition_to_cache(
    cache: &mut CacheState,
    transitions: &Vec<(Address, TransitionAccount)>,
) {
    for (address, account) in transitions {
        let new_storage = account.storage.iter().map(|(k, s)| (*k, s.present_value));
        if let Some(entry) = cache.accounts.get_mut(address) {
            if let Some(new_info) = &account.info {
                assert!(!account.storage_was_destroyed);
                if let Some(read_account) = entry.account.as_mut() {
                    // account is loaded
                    read_account.info = new_info.clone();
                    read_account.storage.extend(new_storage);
                } else {
                    // account is loaded not existing
                    entry.account = Some(PlainAccount {
                        info: new_info.clone(),
                        storage: new_storage.collect(),
                    });
                }
            } else {
                assert!(account.storage_was_destroyed);
                entry.account = None;
            }
            entry.status = account.status;
        } else {
            cache.accounts.insert(
                *address,
                CacheAccount {
                    account: account.info.as_ref().map(|info| PlainAccount {
                        info: info.clone(),
                        storage: new_storage.collect(),
                    }),
                    status: account.status,
                },
            );
        }
    }
}

/// SchedulerDB is used as a database for EVM when falling back to sequential execution.
impl<DB> Database for SchedulerDB<DB>
where
    DB: DatabaseRef,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.load_cache_account(address).map(|account| account.account_info())
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let res = match self.state.cache.contracts.entry(code_hash) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            hash_map::Entry::Vacant(entry) => {
                let code = self.database.code_by_hash_ref(code_hash)?;
                entry.insert(code.clone());
                Ok(code)
            }
        };
        res
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        load_storage(&mut self.state.cache, &self.database, address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        match self.state.block_hashes.entry(number) {
            btree_map::Entry::Occupied(entry) => Ok(*entry.get()),
            btree_map::Entry::Vacant(entry) => {
                let ret = *entry.insert(self.database.block_hash_ref(number)?);

                // prune all hashes that are older than BLOCK_HASH_HISTORY
                let last_block = number.saturating_sub(BLOCK_HASH_HISTORY);
                while let Some(entry) = self.state.block_hashes.first_entry() {
                    if *entry.key() < last_block {
                        entry.remove();
                    } else {
                        break;
                    }
                }

                Ok(ret)
            }
        }
    }
}

impl<DB> DatabaseCommit for SchedulerDB<DB> {
    /// Fall back to sequential execute
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        let transitions = self.state.cache.apply_evm_state(changes);
        self.apply_transition(transitions);
    }
}

/// PartitionDB is used in PartitionExecutor to build EVM and hook the read operations.
/// It maintains the partition internal cache, scheduler_db, and block_hashes.
/// It also records the read set of the current transaction, which will be consumed after the
/// execution of each transaction.
pub(crate) struct PartitionDB<DB> {
    /// The address of the miner
    /// Miner's account may be updated for each transaction, if we add miner's account to the
    /// read/write set, every transaction will be conflict with each other, so we need to
    /// handle miner's account separately.
    pub coinbase: Address,

    /// Cache the state of the partition
    pub cache: CacheState,
    /// The scheduler database, used to load the state of the committed data
    pub scheduler_db: Arc<SchedulerDB<DB>>,
    pub block_hashes: BTreeMap<u64, B256>,

    /// Record the read set of current tx, will be consumed after the execution of each tx
    tx_read_set: AHashMap<LocationAndType, Option<U256>>,

    pub raw_transfer: bool,
}

impl<DB> PartitionDB<DB> {
    pub(crate) fn new(coinbase: Address, scheduler_db: Arc<SchedulerDB<DB>>) -> Self {
        Self {
            coinbase,
            cache: CacheState::new(scheduler_db.state.cache.has_state_clear),
            scheduler_db,
            block_hashes: BTreeMap::new(),
            tx_read_set: AHashMap::new(),
            raw_transfer: false,
        }
    }

    /// consume the read set after evm.transact() for each tx
    pub(crate) fn take_read_set(&mut self) -> AHashMap<LocationAndType, Option<U256>> {
        core::mem::take(&mut self.tx_read_set)
    }

    /// Generate the write set after evm.transact() for each tx
    /// The write set includes the locations of the basic account, code, and storage slots that have
    /// been modified. Returns the write set(exclude miner) and the miner's rewards.
    pub(crate) fn generate_write_set(&self, changes: &mut EvmState) -> LocationSet {
        let mut write_set = AHashSet::new();
        for (address, account) in &mut *changes {
            if account.is_selfdestructed() {
                write_set.insert(LocationAndType::Code(*address));
                // When a contract account is destroyed, its remaining balance is sent to a
                // designated address, and the account’s balance becomes invalid.
                // Defensive programming should be employed to prevent subsequent transactions
                // from attempting to read the contract account’s basic information,
                // which could lead to errors.
                write_set.insert(LocationAndType::Basic(*address));
                continue;
            }

            // If the account is touched, it means that the account's state has been modified
            // during the transaction. This includes changes to the account's balance, nonce,
            // or code. We need to track these changes to ensure the correct state is committed
            // after the transaction.
            if account.is_touched() {
                let has_code = !account.info.is_empty_code_hash();
                // is newly created contract
                let mut new_contract_account = false;

                if match self.cache.accounts.get(address) {
                    Some(read_account) => {
                        read_account.account.as_ref().map_or(true, |read_account| {
                            new_contract_account =
                                has_code && read_account.info.is_empty_code_hash();
                            new_contract_account ||
                                read_account.info.nonce != account.info.nonce ||
                                read_account.info.balance != account.info.balance
                        })
                    }
                    None => {
                        new_contract_account = has_code;
                        true
                    }
                } {
                    write_set.insert(LocationAndType::Basic(*address));
                }
                if new_contract_account {
                    write_set.insert(LocationAndType::Code(*address));
                }
            }

            for (slot, _) in account.changed_storage_slots() {
                write_set.insert(LocationAndType::Storage(*address, *slot));
            }
        }
        write_set
    }

    /// Temporary commit the state change after evm.transact() for each tx
    /// Final commit will be called when the transaction is marked as finality in the validation of
    /// scheduler.
    pub(crate) fn temporary_commit(
        &mut self,
        changes: EvmState,
    ) -> Vec<(Address, TransitionAccount)> {
        self.cache.apply_evm_state(changes)
    }

    pub(crate) fn temporary_commit_transition(
        &mut self,
        transitions: &Vec<(Address, TransitionAccount)>,
    ) {
        apply_transition_to_cache(&mut self.cache, transitions);
    }
}

impl<DB> PartitionDB<DB>
where
    DB: DatabaseRef,
{
    /// If the read set is consistent with the read set of the previous round of execution,
    /// We can reuse the results of the previous round of execution, and no need to re-execute the
    /// transaction.
    pub(crate) fn check_read_set(
        &mut self,
        read_set: &AHashMap<LocationAndType, Option<U256>>,
    ) -> bool {
        let mut visit_account = AHashSet::new();
        for (location, _) in read_set {
            match location {
                LocationAndType::Basic(address) => {
                    if !visit_account.contains(address) {
                        let _ = self.basic(address.clone());
                        visit_account.insert(address.clone());
                    }
                }
                LocationAndType::Storage(address, index) => {
                    // If the account is not loaded, we need to load it from the database.
                    if !visit_account.contains(address) {
                        let _ = self.basic(address.clone());
                        visit_account.insert(address.clone());
                    }
                    let _ = self.storage(address.clone(), index.clone());
                }
                _ => {}
            }
        }
        let new_read_set = self.take_read_set();
        if new_read_set.len() != read_set.len() {
            false
        } else {
            new_read_set
                .iter()
                .all(|(key, value)| read_set.get(key).map_or(false, |v| *value == *v))
        }
    }
}

/// Used to build evm, and hook the read operations
impl<DB> Database for PartitionDB<DB>
where
    DB: DatabaseRef,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 1. read from internal cache
        let result = match self.cache.accounts.entry(address) {
            hash_map::Entry::Vacant(entry) => {
                // 2. read initial state of this round from scheduler cache
                if let Some(account) = self.scheduler_db.state.cache.accounts.get(&address) {
                    Ok(entry.insert(account.clone()).account_info())
                } else {
                    // 3. read from origin database
                    tokio::task::block_in_place(|| self.scheduler_db.database.basic_ref(address))
                        .map(|info| entry.insert(into_cache_account(info)).account_info())
                }
            }
            hash_map::Entry::Occupied(entry) => Ok(entry.get().account_info()),
        };
        let mut balance = None;
        if let Ok(account) = &result {
            if let Some(info) = account {
                if !info.is_empty_code_hash() {
                    self.tx_read_set.insert(LocationAndType::Code(address), None);
                }
                balance = Some(info.balance);
            }
        }
        if address == self.coinbase && !self.raw_transfer {
            self.tx_read_set.entry(LocationAndType::Basic(address)).or_insert(None);
        } else {
            self.tx_read_set.entry(LocationAndType::Basic(address)).or_insert(balance);
        }
        result
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 1. read from internal cache
        let res = match self.cache.contracts.entry(code_hash) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            hash_map::Entry::Vacant(entry) => {
                // 2. read initial state of this round from scheduler cache
                if let Some(code) = self.scheduler_db.state.cache.contracts.get(&code_hash) {
                    return Ok(entry.insert(code.clone()).clone());
                }

                // 3. read from origin database
                let code = tokio::task::block_in_place(|| {
                    self.scheduler_db.database.code_by_hash_ref(code_hash)
                })?;
                entry.insert(code.clone());
                return Ok(code);
            }
        };
        res
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let result = load_storage(&mut self.cache, &self.scheduler_db.database, address, index);
        let mut slot_value = None;
        if let Ok(value) = &result {
            slot_value = Some(value.clone());
        }
        self.tx_read_set.entry(LocationAndType::Storage(address, index)).or_insert(slot_value);

        result
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // FIXME(gravity_nekomoto): too lot repeated code
        match self.block_hashes.entry(number) {
            btree_map::Entry::Occupied(entry) => Ok(*entry.get()),
            btree_map::Entry::Vacant(entry) => {
                // TODO(gravity_nekomoto): read from scheduler_db?
                let ret = *entry.insert(tokio::task::block_in_place(|| {
                    self.scheduler_db.database.block_hash_ref(number)
                })?);

                // prune all hashes that are older then BLOCK_HASH_HISTORY
                let last_block = number.saturating_sub(BLOCK_HASH_HISTORY);
                while let Some(entry) = self.block_hashes.first_entry() {
                    if *entry.key() < last_block {
                        entry.remove();
                    } else {
                        break;
                    }
                }

                Ok(ret)
            }
        }
    }
}
