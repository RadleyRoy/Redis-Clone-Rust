//! The concrete value types stored in the database: lists, sets and sorted
//! sets. Each type owns its data and exposes only the operations the command
//! layer needs, keeping the storage details encapsulated.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use ordered_float::OrderedFloat;

/// Translate Redis-style inclusive `[start, stop]` indices into a concrete
/// `(start, stop)` pair of `usize` offsets.
///
/// Redis indices may be negative, counting back from the end (`-1` is the last
/// element), and out-of-range values are clamped rather than rejected. Returns
/// `None` when the range selects no elements, which lets callers avoid any
/// risk of arithmetic underflow.
fn resolve_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = len as i64;
    let normalize = |index: i64| if index < 0 { index + len } else { index };

    let start = normalize(start).max(0);
    let stop = normalize(stop).min(len - 1);

    if start > stop || start >= len {
        return None;
    }
    Some((start as usize, stop as usize))
}

/// A Redis list, backed by a double-ended queue for O(1) pushes and pops at
/// both ends.
#[derive(Default)]
pub struct RList {
    list: VecDeque<String>,
}

impl RList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lpush(&mut self, value: String) {
        self.list.push_front(value);
    }

    pub fn rpush(&mut self, value: String) {
        self.list.push_back(value);
    }

    pub fn lpop(&mut self) -> Option<String> {
        self.list.pop_front()
    }

    pub fn rpop(&mut self) -> Option<String> {
        self.list.pop_back()
    }

    /// Returns the elements in the inclusive `[start, stop]` range, honouring
    /// negative indices. An empty range yields an empty vector.
    pub fn lrange(&self, start: i64, stop: i64) -> Vec<String> {
        match resolve_range(start, stop, self.list.len()) {
            Some((start, stop)) => self
                .list
                .iter()
                .skip(start)
                .take(stop - start + 1)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }
}

/// A Redis set of unique string members.
#[derive(Default)]
pub struct RSet {
    set: HashSet<String>,
}

impl RSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a member, returning `true` if it was not already present.
    pub fn sadd(&mut self, value: String) -> bool {
        self.set.insert(value)
    }

    /// Removes a member, returning `true` if it was present.
    pub fn srem(&mut self, value: &str) -> bool {
        self.set.remove(value)
    }

    pub fn smembers(&self) -> Vec<String> {
        self.set.iter().cloned().collect()
    }

    pub fn sismember(&self, value: &str) -> bool {
        self.set.contains(value)
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

/// A single (member, score) pair, ordered by score and then lexicographically
/// by member so that ties have a stable, deterministic order.
#[derive(Clone, Eq)]
struct SortedMember {
    member: String,
    score: OrderedFloat<f64>,
}

impl PartialEq for SortedMember {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member && self.score == other.score
    }
}

impl Ord for SortedMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for SortedMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A Redis sorted set. Two indexes are kept in sync: a `HashMap` for O(1)
/// score lookup by member, and a `BTreeSet` that keeps members ordered by
/// score for range queries.
#[derive(Default)]
pub struct RSortedSet {
    members: HashMap<String, OrderedFloat<f64>>,
    sorted: BTreeSet<SortedMember>,
}

impl RSortedSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `member` with `score`, or updates its score if it already exists.
    /// Returns `true` only when the member is newly added, matching Redis'
    /// `ZADD` reply which counts added (not updated) members.
    pub fn zadd(&mut self, score: f64, member: String) -> bool {
        let score = OrderedFloat(score);
        let is_new = match self.members.get(&member) {
            Some(&old_score) if old_score == score => return false,
            Some(&old_score) => {
                self.sorted.remove(&SortedMember {
                    member: member.clone(),
                    score: old_score,
                });
                false
            }
            None => true,
        };

        self.sorted.insert(SortedMember {
            member: member.clone(),
            score,
        });
        self.members.insert(member, score);
        is_new
    }

    /// Removes `member`, returning `true` if it was present.
    pub fn zrem(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.sorted.remove(&SortedMember {
                member: member.to_string(),
                score,
            });
            true
        } else {
            false
        }
    }

    /// Returns members in the inclusive `[start, stop]` rank range (ascending
    /// by score), honouring negative indices.
    pub fn zrange(&self, start: i64, stop: i64) -> Vec<String> {
        match resolve_range(start, stop, self.sorted.len()) {
            Some((start, stop)) => self
                .sorted
                .iter()
                .skip(start)
                .take(stop - start + 1)
                .map(|entry| entry.member.clone())
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn zscore(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|score| score.into_inner())
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn list_range_handles_negative_and_out_of_bounds() {
        let mut list = RList::new();
        for value in ["a", "b", "c", "d"] {
            list.rpush(value.to_string());
        }
        assert_eq!(list.lrange(0, -1), owned(&["a", "b", "c", "d"]));
        assert_eq!(list.lrange(-2, -1), owned(&["c", "d"]));
        assert_eq!(list.lrange(0, 100), owned(&["a", "b", "c", "d"]));
        assert_eq!(list.lrange(2, 1), Vec::<String>::new());
        assert_eq!(RList::new().lrange(0, -1), Vec::<String>::new());
    }

    #[test]
    fn set_add_remove_and_membership() {
        let mut set = RSet::new();
        assert!(set.sadd("x".to_string()));
        assert!(!set.sadd("x".to_string()));
        assert!(set.sismember("x"));
        assert!(set.srem("x"));
        assert!(set.is_empty());
    }

    #[test]
    fn sorted_set_orders_by_score_and_counts_only_new_members() {
        let mut zset = RSortedSet::new();
        assert!(zset.zadd(1.0, "a".to_string()));
        assert!(!zset.zadd(2.0, "a".to_string())); // update, not an add
        assert!(zset.zadd(0.5, "b".to_string()));
        assert_eq!(zset.zrange(0, -1), owned(&["b", "a"]));
        assert_eq!(zset.zscore("a"), Some(2.0));
        assert_eq!(zset.zscore("missing"), None);
    }
}
