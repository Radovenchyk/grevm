//! # grevm
//!
//! `grevm` is a library for executing and managing Ethereum Virtual Machine (EVM) transactions
//! with support for parallel execution and custom scheduling.
//!
//! ## Modules
//!
//! - `hint`: Contains hint-related functionalities.
//! - `partition`: Manages partitioning of transactions.
//! - `scheduler`: Handles scheduling of transactions for execution.
//! - `storage`: Manages storage-related operations.
//! - `tx_dependency`: Handles transaction dependencies.

use lazy_static::lazy_static;
use revm::primitives::{Address, EVMError, ExecutionResult, U256};
use revm::TransitionAccount;
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use revm::precompile::HashMap;
use tokio::runtime::{Builder, Runtime};
mod hint;
mod partition;
mod scheduler;
mod storage;
mod tx_dependency;

lazy_static! {
    static ref CPU_CORES: usize = thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
}

lazy_static! {
    static ref GREVM_RUNTIME: Runtime = Builder::new_multi_thread()
        // .worker_threads(1) // for debug
        .worker_threads(thread::available_parallelism().map(|n| n.get() * 2).unwrap_or(8))
        .thread_name("grevm-tokio-runtime")
        .enable_all()
        .build()
        .unwrap();
}

use crate::storage::LazyUpdateValue;
pub use scheduler::*;

/// The maximum number of rounds for transaction execution.
static MAX_NUM_ROUND: usize = 3;

/// Alias for `usize`, representing the ID of a partition.
type PartitionId = usize;

/// Alias for `usize`, representing the ID of a transaction.
type TxId = usize;

/// Represents the location and type of a resource in the EVM.
///
/// This enum is used to specify different types of locations within the EVM,
/// such as basic addresses, storage slots, and contract code.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum LocationAndType {
    /// Represents a basic address location(for EOA).
    Basic(Address),

    /// Represents a storage location with an address and a storage slot(for CA).
    Storage(Address, U256),

    /// Represents a contract code location with an address(for CA).
    Code(Address),
}

/// Represents the status of a transaction during its lifecycle.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum TransactionStatus {
    /// Transaction that has not yet been run once.
    Initial,

    /// Transaction that has been executed.
    Executed,

    /// Transaction that is validated but not the continuous ID.
    Unconfirmed,

    /// Transaction that is conflicted and needs to be rerun.
    Conflict,

    /// Transaction that can skip validation.
    SkipValidation,

    /// Transaction that is validated and is the continuous ID.
    Finality,
}

/// Represents errors that can occur within the `grevm` library.
///
/// This enum encapsulates various types of errors that can be encountered
/// during the execution and management of EVM transactions.
#[derive(Debug)]
pub enum GrevmError<DBError> {
    /// Error originating from the EVM(within EVM).
    EvmError(EVMError<DBError>),

    /// Error occurring during the execution of a transaction(within grevm).
    ExecutionError(String),

    /// Error indicating an unreachable state or code path.
    UnreachableError(String),
}

impl<DBError: Display> Display for GrevmError<DBError> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GrevmError::EvmError(e) => write!(f, "EVM Error: {}", e),
            GrevmError::ExecutionError(e) => write!(f, "Execution Error: {}", e),
            GrevmError::UnreachableError(e) => write!(f, "Unreachable Error: {}", e),
        }
    }
}

/// Represents the result of an EVM transaction execution along with state transitions.
///
/// This struct encapsulates the outcome of executing a transaction, including the execution
/// result, state transitions, and any rewards to the miner.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResultAndTransition {
    /// Status of execution.
    pub result: Option<ExecutionResult>,

    /// State that got updated.
    pub transition: Vec<(Address, TransitionAccount)>,

    /// Rewards to miner.
    pub miner_update: LazyUpdateValue,
}

/// Utility function for parallel execution using fork-join pattern.
///
/// This function divides the work into partitions and executes the provided closure `f`
/// in parallel across multiple threads. The number of partitions can be specified, or it
/// will default to twice the number of CPU cores plus one.
///
/// # Arguments
///
/// * `num_elements` - The total number of elements to process.
/// * `num_partitions` - Optional number of partitions to divide the work into.
/// * `f` - A closure that takes three arguments: the start index, the end index, and the partition index.
///
/// # Example
///
/// ```
/// use grevm::fork_join_util;
/// fork_join_util(100, Some(4), |start, end, index| {
///     println!("Partition {}: processing elements {} to {}", index, start, end);
/// });
/// ```
pub fn fork_join_util<'scope, F>(num_elements: usize, num_partitions: Option<usize>, f: F)
where
    F: Fn(usize, usize, usize) + Send + Sync + 'scope,
{
    let parallel_cnt = num_partitions.unwrap_or(*CPU_CORES * 2 + 1);
    let partition_id = AtomicUsize::new(0);
    let remaining = num_elements % parallel_cnt;
    let chunk_size = num_elements / parallel_cnt;
    thread::scope(|scope| {
        for _ in 0..parallel_cnt {
            scope.spawn(|| {
                let partition_id = partition_id.fetch_add(1, Ordering::SeqCst);
                let start_pos = chunk_size * partition_id + min(partition_id, remaining);
                let mut end_pos = start_pos + chunk_size;
                if partition_id < remaining {
                    end_pos += 1;
                }
                f(start_pos, end_pos, partition_id);
            });
        }
    });
}

#[test]
fn test_fork_join_util() {
    let num_elements: usize = 9008;
    let num_partitions: usize = 10;
    let groups: Arc<Mutex<HashMap<usize, (usize, usize)>>> = Arc::new(Mutex::new(HashMap::new()));
    fork_join_util(num_elements, Some(num_partitions), |start_pos, end_pos, index| {
        groups.lock().unwrap().insert(index, (start_pos, end_pos));
    });

    let target_group = HashMap::from([
        (0, (0, 901)),
        (1, (901, 1802)),
        (2, (1802, 2703)),
        (3, (2703, 3604)),
        (4, (3604, 4505)),
        (5, (4505, 5406)),
        (6, (5406, 6307)),
        (7, (6307, 7208)),
        (8, (7208, 8108)),
        (9, (8108, 9008)),
    ]);
    assert_eq!(*groups.lock().unwrap(), target_group);
}