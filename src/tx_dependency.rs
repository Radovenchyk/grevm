use crate::TxId;
use ahash::AHashSet as HashSet;
use std::{collections::BTreeSet, mem};

pub struct TxDependency {
    dependent_txs: Vec<HashSet<TxId>>,
    affect_txs: Vec<HashSet<TxId>>,
    no_dep_txs: BTreeSet<TxId>,
}

impl TxDependency {
    pub fn new(num_txs: usize) -> Self {
        Self {
            dependent_txs: vec![HashSet::new(); num_txs],
            affect_txs: vec![HashSet::new(); num_txs],
            no_dep_txs: (0..num_txs).collect(),
        }
    }

    pub fn create(
        dependent_txs: Vec<HashSet<TxId>>,
        affect_txs: Vec<HashSet<TxId>>,
        no_dep_txs: BTreeSet<TxId>,
    ) -> Self {
        Self { dependent_txs, affect_txs, no_dep_txs }
    }

    pub fn next(&mut self) -> Option<TxId> {
        self.no_dep_txs.pop_first()
    }

    pub fn remove(&mut self, txid: TxId) {
        let affect_txs = mem::take(&mut self.affect_txs[txid]);
        for affect_tx in affect_txs {
            self.dependent_txs[affect_tx].remove(&txid);
            if self.dependent_txs[affect_tx].is_empty() {
                self.no_dep_txs.insert(affect_tx);
            }
        }
    }

    pub fn add<D: IntoIterator<Item = TxId>>(&mut self, txid: TxId, dependent_txs: D) {
        self.dependent_txs[txid].extend(dependent_txs);
        if self.dependent_txs[txid].is_empty() {
            self.no_dep_txs.insert(txid);
        }
    }

    pub fn update_dependency(&mut self, from: TxId, to: TxId) {
        self.dependent_txs[from].insert(to);
        self.affect_txs[to].insert(from);
        self.no_dep_txs.remove(&from);
    }
}
