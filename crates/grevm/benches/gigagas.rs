#![allow(missing_docs)]

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

#[path = "../tests/common/mod.rs"]
pub mod common;

use common::storage::InMemoryDB;
use reth_chainspec::NamedChain;
use reth_grevm::GrevmScheduler;
use revm_primitives::alloy_primitives::U160;
use revm_primitives::{Address, Env, SpecId, TransactTo, TxEnv, U256};

const GIGA_GAS: u64 = 1_000_000_000;

fn bench(c: &mut Criterion, name: &str, db: InMemoryDB, txs: Vec<TxEnv>) {
    let mut env = Env::default();
    env.cfg.chain_id = NamedChain::Mainnet.into();
    env.block.coinbase = Address::from(U160::from(common::MINER_ADDRESS));
    let db = Arc::new(db);

    let mut group = c.benchmark_group(name);
    group.bench_function("Sequential", |b| {
        b.iter(|| {
            common::execute_revm_sequential(
                black_box(db.clone()),
                black_box(SpecId::LATEST),
                black_box(env.clone()),
                black_box(txs.clone()),
            )
        })
    });
    group.bench_function("Grevm Parallel", |b| {
        b.iter(|| {
            let executor = GrevmScheduler::new(
                black_box(SpecId::LATEST),
                black_box(env.clone()),
                black_box(db.clone()),
                black_box(txs.clone()),
            );
            executor.adaptive_execute()
        })
    });
    group.bench_function("Grevm Sequential", |b| {
        b.iter(|| {
            let executor = GrevmScheduler::new(
                black_box(SpecId::LATEST),
                black_box(env.clone()),
                black_box(db.clone()),
                black_box(txs.clone()),
            );
            executor.sequential_execute()
        })
    });
    group.finish();
}

fn bench_raw_transfers(c: &mut Criterion, db_latency_us: u64) {
    let block_size = (GIGA_GAS as f64 / common::TRANSFER_GAS_LIMIT as f64).ceil() as usize;
    let accounts = common::mock_block_accounts(common::START_ADDRESS, block_size);
    let mut db = InMemoryDB::new(accounts, Default::default(), Default::default());
    db.latency_us = db_latency_us;
    bench(
        c,
        "Independent Raw Transfers",
        db,
        (1..=block_size)
            .map(|i| {
                let address = Address::from(U160::from(common::START_ADDRESS + i));
                TxEnv {
                    caller: address,
                    transact_to: TransactTo::Call(address),
                    value: U256::from(1),
                    gas_limit: common::TRANSFER_GAS_LIMIT,
                    gas_price: U256::from(1),
                    ..TxEnv::default()
                }
            })
            .collect::<Vec<_>>(),
    );
}

fn benchmark_gigagas(c: &mut Criterion) {
    // TODO(gravity): Create options from toml file if there are more
    let db_latency_us = std::env::var_os("DB_LATENCY_US")
        .map(|s| s.to_string_lossy().parse().unwrap())
        .unwrap_or(0);
    bench_raw_transfers(c, db_latency_us);
}

criterion_group!(benches, benchmark_gigagas);
criterion_main!(benches);
