//! Helpers for property tests.

use std::{fmt::Debug, ops::Range};

use proptest::prelude::*;
use smol_str::SmolStr;

use crate::{
    columnar_2::rowblock::row_ops::Key as RowOpKey,
    types::{OpId, Key, ElemId}
};

#[derive(Clone, Debug)]
pub(crate) struct SpliceScenario<T> {
    pub(crate) initial_values: Vec<T>,
    pub(crate) replace_range: Range<usize>,
    pub(crate) replacements: Vec<T>,
}

impl<T: Debug + PartialEq + Clone> SpliceScenario<T> {
    pub(crate) fn check(&self, results: Vec<T>) {
        let mut expected = self
            .initial_values
            .clone();
        expected.splice(self.replace_range.clone(), self.replacements.clone());
        assert_eq!(expected, results)
    }
}

pub(crate) fn splice_scenario<S: Strategy<Value = T> + Clone, T: Debug + Clone + 'static>(
    item_strat: S,
) -> impl Strategy<Value = SpliceScenario<T>> {
    (
        proptest::collection::vec(item_strat.clone(), 0..100),
        proptest::collection::vec(item_strat, 0..10),
    )
        .prop_flat_map(move |(values, to_splice)| {
            if values.len() == 0 {
                Just(SpliceScenario {
                    initial_values: values.clone(),
                    replace_range: 0..0,
                    replacements: to_splice.clone(),
                })
                .boxed()
            } else {
                // This is somewhat awkward to write because we have to carry the `values` and
                // `to_splice` through as `Just(..)` to please the borrow checker.
                (0..values.len(), Just(values), Just(to_splice))
                    .prop_flat_map(move |(replace_range_start, values, to_splice)| {
                        (
                            0..(values.len() - replace_range_start),
                            Just(values),
                            Just(to_splice),
                        )
                            .prop_map(
                                move |(replace_range_len, values, to_splice)| SpliceScenario {
                                    initial_values: values.clone(),
                                    replace_range: replace_range_start
                                        ..(replace_range_start + replace_range_len),
                                    replacements: to_splice.clone(),
                                },
                            )
                    })
                    .boxed()
            }
        })
}

pub(crate) fn opid() -> impl Strategy<Value = OpId> + Clone {
    (0..(i64::MAX as usize), 0..(i64::MAX as u64)).prop_map(|(actor, ctr)| OpId(ctr, actor))
}

pub(crate) fn elemid() -> impl Strategy<Value = ElemId> + Clone {
    opid().prop_map(ElemId)
}

pub(crate) fn key() -> impl Strategy<Value = Key> + Clone {
    prop_oneof!{
        elemid().prop_map(Key::Seq),
        (0..(i64::MAX as usize)).prop_map(Key::Map),
    }
}

fn smol_str() -> impl Strategy<Value = SmolStr> + Clone {
    any::<String>().prop_map(SmolStr::from)
}

pub(crate) fn row_op_key() -> impl Strategy<Value = RowOpKey> + Clone {
    prop_oneof!{
        elemid().prop_map(RowOpKey::Elem),
        smol_str().prop_map(RowOpKey::Prop),
    }
}
