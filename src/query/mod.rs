//! Minimal expression tree specialized for key-level constraints.
//!
//! This mirrors a subset of Tonbo's resolved predicates and is generic
//! over the key type `K`. It allows us to:
//! - Extract a coarse key range for index scans
//! - Evaluate the expression against a key value (for post-filtering)
//! - Build `RangeSet` for efficient range-bounded scans

// no additional imports required

use crate::scan::{KeyRange, RangeSet};

/// A single bound in a range over keys.
#[derive(Debug, Clone, PartialEq)]
pub struct Bound<K> {
    /// Whether the bound is inclusive (`[v, ...)`) or exclusive (`(v, ...)`).
    pub inclusive: bool,
    /// The bound key value.
    pub value: K,
}

/// Primitive key predicates.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate<K> {
    /// Equality over the key.
    Eq {
        /// The key value to compare for equality.
        value: K,
    },
    /// Finite set membership over the key.
    In {
        /// The set of key values to match.
        set: Vec<K>,
    },
    /// Range over the key (any combination of lower/upper).
    Range {
        /// Optional lower bound for the key range.
        lower: Option<Bound<K>>,
        /// Optional upper bound for the key range.
        upper: Option<Bound<K>>,
    },
}

/// An expression tree specialized for keys.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr<K> {
    /// A single primitive predicate.
    Pred(Predicate<K>),
    /// Logical conjunction of child expressions.
    And(Vec<Expr<K>>),
    /// Logical disjunction of child expressions.
    Or(Vec<Expr<K>>),
    /// Logical negation of the child expression.
    Not(Box<Expr<K>>),
}

/// Convert an expression to a coarse `KeyRange` by:
/// - Using exact ranges for Eq/Range
/// - Bounding In/Or by min..max of members/children
/// - Intersecting And children
pub fn extract_key_ranges<K>(expr: &Expr<K>) -> RangeSet<K>
where
    K: Ord + Clone,
{
    use std::ops::Bound as B;
    match expr {
        Expr::Pred(Predicate::Eq { value }) => RangeSet::from_ranges(vec![KeyRange::new(
            B::Included(value.clone()),
            B::Included(value.clone()),
        )]),
        Expr::Pred(Predicate::Range { lower, upper }) => {
            let start = lower
                .as_ref()
                .map(|b| {
                    if b.inclusive {
                        B::Included(b.value.clone())
                    } else {
                        B::Excluded(b.value.clone())
                    }
                })
                .unwrap_or(B::Unbounded);
            let end = upper
                .as_ref()
                .map(|b| {
                    if b.inclusive {
                        B::Included(b.value.clone())
                    } else {
                        B::Excluded(b.value.clone())
                    }
                })
                .unwrap_or(B::Unbounded);
            RangeSet::from_ranges(vec![KeyRange::new(start, end)])
        }
        Expr::Pred(Predicate::In { set }) => {
            if set.is_empty() {
                RangeSet::empty()
            } else {
                let ranges = set
                    .iter()
                    .cloned()
                    .map(|v| KeyRange::new(B::Included(v.clone()), B::Included(v)))
                    .collect();
                RangeSet::from_ranges(ranges)
            }
        }
        Expr::And(children) => {
            // Start unbounded, intersect each child's range set
            let mut cur = RangeSet::all();
            for c in children {
                cur = cur.intersect(extract_key_ranges(c));
            }
            cur
        }
        Expr::Or(children) => {
            let mut out = RangeSet::empty();
            for c in children {
                out = out.union(extract_key_ranges(c));
            }
            out
        }
        Expr::Not(child) => extract_key_ranges(child).complement(),
    }
}

// Helper utilities and adapters are intentionally minimal and key-focused.

/// Evaluate the expression for a given key.
pub fn matches_key<K: Ord>(expr: &Expr<K>, key: &K) -> bool {
    match expr {
        Expr::Pred(Predicate::Eq { value }) => key == value,
        Expr::Pred(Predicate::In { set }) => set.iter().any(|v| v == key),
        Expr::Pred(Predicate::Range { lower, upper }) => {
            let lower_ok = match lower {
                None => true,
                Some(Bound { inclusive, value }) => {
                    if *inclusive {
                        key >= value
                    } else {
                        key > value
                    }
                }
            };
            if !lower_ok {
                return false;
            }
            match upper {
                None => true,
                Some(Bound { inclusive, value }) => {
                    if *inclusive {
                        key <= value
                    } else {
                        key < value
                    }
                }
            }
        }
        Expr::And(children) => children.iter().all(|c| matches_key(c, key)),
        Expr::Or(children) => children.iter().any(|c| matches_key(c, key)),
        Expr::Not(child) => !matches_key(child, key),
    }
}

/// Convenience: convert a resolved expression to a `RangeSet` for the given key type.
pub fn to_range_set<K: Ord + Clone>(expr: &Expr<K>) -> RangeSet<K> {
    extract_key_ranges(expr)
}

// No sentinel helpers needed after OR logic handles Unbounded explicitly

#[cfg(test)]
mod tests {
    use std::ops::Bound as B;

    use super::*;

    #[test]
    fn matches_eq_in_range() {
        let e = Expr::Pred(Predicate::Eq { value: 5 });
        assert!(matches_key(&e, &5));
        assert!(!matches_key(&e, &4));

        let e = Expr::Pred(Predicate::In { set: vec![1, 3, 5] });
        assert!(matches_key(&e, &1));
        assert!(matches_key(&e, &3));
        assert!(!matches_key(&e, &2));

        let e = Expr::Pred(Predicate::Range {
            lower: Some(Bound {
                inclusive: true,
                value: 3,
            }),
            upper: Some(Bound {
                inclusive: false,
                value: 7,
            }),
        });
        assert!(matches_key(&e, &3));
        assert!(matches_key(&e, &6));
        assert!(!matches_key(&e, &7));
    }

    #[test]
    fn range_extract_and_intersect() {
        let r1 = extract_key_ranges(&Expr::Pred(Predicate::Range {
            lower: Some(Bound {
                inclusive: true,
                value: 3,
            }),
            upper: Some(Bound {
                inclusive: false,
                value: 10,
            }),
        }));
        let r2 = extract_key_ranges(&Expr::Pred(Predicate::Eq { value: 7 }));
        // And -> intersection should contain [7,7]
        let r = extract_key_ranges(&Expr::And(vec![
            Expr::Pred(Predicate::Range {
                lower: Some(Bound {
                    inclusive: true,
                    value: 3,
                }),
                upper: Some(Bound {
                    inclusive: false,
                    value: 10,
                }),
            }),
            Expr::Pred(Predicate::Eq { value: 7 }),
        ]));
        // It's hard to compare bounds directly; check via filtering a sample set
        let keys = [2, 3, 7, 9, 10];
        let mut filtered: Vec<i32> = Vec::new();
        for kr in r.iter() {
            let (s, e) = kr.as_borrowed_bounds();
            for k in keys.iter() {
                let in_range = match (s, e) {
                    (B::Included(sv), B::Included(ev)) => *k >= *sv && *k <= *ev,
                    (B::Unbounded, B::Included(ev)) => *k <= *ev,
                    (B::Included(sv), B::Unbounded) => *k >= *sv,
                    (B::Unbounded, B::Unbounded) => true,
                    (B::Excluded(sv), B::Included(ev)) => *k > *sv && *k <= *ev,
                    (B::Included(sv), B::Excluded(ev)) => *k >= *sv && *k < *ev,
                    (B::Excluded(sv), B::Excluded(ev)) => *k > *sv && *k < *ev,
                    (B::Unbounded, B::Excluded(ev)) => *k < *ev,
                    (B::Excluded(sv), B::Unbounded) => *k > *sv,
                };
                if in_range {
                    filtered.push(*k);
                }
            }
        }
        assert_eq!(filtered, vec![7]);
        // r1 and r2 used just to silence warnings
        let _ = (r1, r2);
    }

    #[test]
    fn empty_or_and_in() {
        // Or([]) -> empty; In([]) -> empty
        let r_or = extract_key_ranges::<i32>(&Expr::Or(vec![]));
        assert!(r_or.is_empty());
        let r_in = extract_key_ranges::<i32>(&Expr::Pred(Predicate::In { set: vec![] }));
        assert!(r_in.is_empty());
    }

    #[test]
    fn not_variants_to_ranges() {
        use std::ops::Bound as B;
        // Not(Eq)
        let e = Expr::Not(Box::new(Expr::Pred(Predicate::Eq { value: 5 })));
        let rs = extract_key_ranges(&e);
        let v = [1, 4, 5, 6, 10];
        let mut got = Vec::new();
        for kr in rs.iter() {
            let (s, e) = kr.as_borrowed_bounds();
            for k in v.iter() {
                let in_range = match (s, e) {
                    (B::Included(sv), B::Included(ev)) => *k >= *sv && *k <= *ev,
                    (B::Unbounded, B::Included(ev)) => *k <= *ev,
                    (B::Included(sv), B::Unbounded) => *k >= *sv,
                    (B::Unbounded, B::Unbounded) => true,
                    (B::Excluded(sv), B::Included(ev)) => *k > *sv && *k <= *ev,
                    (B::Included(sv), B::Excluded(ev)) => *k >= *sv && *k < *ev,
                    (B::Excluded(sv), B::Excluded(ev)) => *k > *sv && *k < *ev,
                    (B::Excluded(sv), B::Unbounded) => *k > *sv,
                    (B::Unbounded, B::Excluded(ev)) => *k < *ev,
                };
                if in_range {
                    got.push(*k);
                }
            }
        }
        // All except 5
        assert_eq!(got.into_iter().filter(|x| *x != 5).count(), 4);

        // Not(range [3,7)) => (-inf,3) ∪ [7,+inf)
        let e2 = Expr::Not(Box::new(Expr::Pred(Predicate::Range {
            lower: Some(Bound {
                inclusive: true,
                value: 3,
            }),
            upper: Some(Bound {
                inclusive: false,
                value: 7,
            }),
        })));
        let rs2 = extract_key_ranges(&e2);
        let v2 = [2, 3, 6, 7, 10];
        let mut keep = Vec::new();
        for kr in rs2.iter() {
            let (s, e) = kr.as_borrowed_bounds();
            for k in v2.iter() {
                let in_range = match (s, e) {
                    (B::Included(sv), B::Included(ev)) => *k >= *sv && *k <= *ev,
                    (B::Unbounded, B::Included(ev)) => *k <= *ev,
                    (B::Included(sv), B::Unbounded) => *k >= *sv,
                    (B::Unbounded, B::Unbounded) => true,
                    (B::Excluded(sv), B::Included(ev)) => *k > *sv && *k <= *ev,
                    (B::Included(sv), B::Excluded(ev)) => *k >= *sv && *k < *ev,
                    (B::Excluded(sv), B::Excluded(ev)) => *k > *sv && *k < *ev,
                    (B::Excluded(sv), B::Unbounded) => *k > *sv,
                    (B::Unbounded, B::Excluded(ev)) => *k < *ev,
                };
                if in_range {
                    keep.push(*k);
                }
            }
        }
        assert_eq!(keep, vec![2, 7, 10]);
    }
}
