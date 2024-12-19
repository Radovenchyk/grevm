#![allow(missing_docs)]

mod common;
use std::sync::Arc;

use common::storage::InMemoryDB;
use grevm::{Scheduler, StateAsyncCommit};
use metrics_util::debugging::DebuggingRecorder;
use revm_primitives::{EnvWithHandlerCfg, TxEnv};

/// Return gas used
fn test_execute(
    env: EnvWithHandlerCfg,
    txs: Vec<TxEnv>,
    db: InMemoryDB,
    dump_transition: bool,
) -> u64 {
    let txs = Arc::new(txs);
    let db = Arc::new(db);

    let reth_result =
        common::execute_revm_sequential(db.clone(), env.spec_id(), env.env.as_ref().clone(), &*txs)
            .unwrap();

    // create registry for metrics
    let recorder = DebuggingRecorder::new();
    let parallel_result = metrics::with_local_recorder(&recorder, || {
        let commiter = StateAsyncCommit::new(env.block.coinbase, db.as_ref());
        let mut executor = Scheduler::new(env.spec_id(), *env.env, txs, db, commiter, true);
        executor.parallel_execute(None).unwrap();

        let snapshot = recorder.snapshotter().snapshot();
        for (key, _, _, value) in snapshot.into_vec() {
            println!("metrics: {} => value: {:?}", key.key().name(), value);
        }
        executor.with_commiter(|commiter| commiter.take_result())
    });

    common::compare_result_and_state(&reth_result, &parallel_result);
    reth_result.iter().map(|r| r.result.gas_used()).sum()
}

#[test]
fn mainnet() {
    if let Ok(block_number) = std::env::var("BLOCK_NUMBER").map(|s| s.parse().unwrap()) {
        // Test a specific block
        let bytecodes = common::load_bytecodes_from_disk();
        let (env, txs, mut db) = common::load_block_from_disk(block_number);
        if db.bytecodes.is_empty() {
            // Use the global bytecodes if the block doesn't have its own
            db.bytecodes = bytecodes.clone();
        }
        let dump_transition = std::env::var("DUMP_TRANSITION").is_ok();
        test_execute(env, txs, db, dump_transition);
        return;
    }

    common::for_each_block_from_disk(|env, txs, db| {
        let number = env.env.block.number;
        let num_txs = txs.len();
        println!("Test Block {number}");
        let gas_used = test_execute(env, txs, db, false);
        println!("Test Block {number} done({num_txs} txs, {gas_used} gas)");
    });
}
