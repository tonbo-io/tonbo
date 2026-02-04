use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Version {
    commit_ts: u64,
    tombstone: bool,
    value: Option<i64>,
}

impl Version {
    fn put(commit_ts: u64, value: i64) -> Self {
        Self {
            commit_ts,
            tombstone: false,
            value: Some(value),
        }
    }

    fn delete(commit_ts: u64) -> Self {
        Self {
            commit_ts,
            tombstone: true,
            value: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleVersion {
    commit_ts: u64,
    tombstone: bool,
    value: Option<i64>,
}

impl From<&Version> for VisibleVersion {
    fn from(version: &Version) -> Self {
        Self {
            commit_ts: version.commit_ts,
            tombstone: version.tombstone,
            value: version.value,
        }
    }
}

#[derive(Default, Debug)]
struct MvccOracle {
    versions: BTreeMap<String, Vec<Version>>,
}

impl MvccOracle {
    // Phase 0 invariants are type-agnostic, so we intentionally keep values as i64 for clarity.
    fn put(&mut self, key: impl Into<String>, commit_ts: u64, value: i64) {
        self.versions
            .entry(key.into())
            .or_default()
            .push(Version::put(commit_ts, value));
    }

    fn delete(&mut self, key: impl Into<String>, commit_ts: u64) {
        self.versions
            .entry(key.into())
            .or_default()
            .push(Version::delete(commit_ts));
    }

    fn visible_version(&self, key: &str, snapshot_ts: u64) -> Option<VisibleVersion> {
        let versions = self.versions.get(key)?;
        let mut best: Option<VisibleVersion> = None;
        for version in versions {
            if version.commit_ts > snapshot_ts {
                continue;
            }
            match &best {
                None => best = Some(VisibleVersion::from(version)),
                Some(best_version) => {
                    if version.commit_ts > best_version.commit_ts {
                        best = Some(VisibleVersion::from(version));
                    } else if version.commit_ts == best_version.commit_ts
                        && version.tombstone
                        && !best_version.tombstone
                    {
                        best = Some(VisibleVersion::from(version));
                    }
                }
            }
        }
        best
    }

    fn get(&self, key: &str, snapshot_ts: u64) -> Option<i64> {
        self.visible_version(key, snapshot_ts).and_then(|version| {
            if version.tombstone {
                None
            } else {
                version.value
            }
        })
    }

    fn scan_range(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        snapshot_ts: u64,
    ) -> Vec<(String, i64)> {
        use std::ops::Bound;

        let start_bound = match start {
            Some(bound) => Bound::Included(bound),
            None => Bound::Unbounded,
        };
        let end_bound = match end {
            Some(bound) => Bound::Included(bound),
            None => Bound::Unbounded,
        };

        let mut rows = Vec::new();
        for (key, _) in self.versions.range::<str, _>((start_bound, end_bound)) {
            if let Some(value) = self.get(key, snapshot_ts) {
                rows.push((key.clone(), value));
            }
        }
        rows
    }
}

#[test]
fn mvcc_oracle_visible_version_prefers_tombstone() {
    let mut oracle = MvccOracle::default();
    oracle.put("a", 5, 11);
    oracle.delete("a", 5);
    oracle.put("a", 7, 13);
    oracle.put("b", 3, 21);
    oracle.delete("b", 4);
    oracle.put("c", 2, 31);

    assert_eq!(oracle.visible_version("a", 4), None);
    assert_eq!(
        oracle.visible_version("a", 5),
        Some(VisibleVersion {
            commit_ts: 5,
            tombstone: true,
            value: None,
        })
    );
    assert_eq!(oracle.get("a", 6), None);
    assert_eq!(oracle.get("a", 7), Some(13));

    assert_eq!(oracle.get("b", 3), Some(21));
    assert_eq!(oracle.get("b", 4), None);
    assert_eq!(oracle.get("b", 5), None);

    let scan = oracle.scan_range(Some("a"), Some("c"), 6);
    assert_eq!(scan, vec![("c".to_string(), 31)]);
}
