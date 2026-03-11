use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use ordered_float::OrderedFloat;

pub struct RList {
    pub list: VecDeque<String>,
}

impl RList {
    pub fn new() -> Self {
        RList {
            list: VecDeque::new(),
        }
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

    pub fn lrange(&self, start: usize, end: usize) -> Vec<String> {
        self.list
            .iter()
            .skip(start)
            .take(end - start + 1)
            .cloned()
            .collect()
    }
}

pub struct RSet {
    pub set: HashSet<String>,
}

impl RSet {
    pub fn new() -> Self {
        RSet {
            set: HashSet::new(),
        }
    }

    pub fn sadd(&mut self, value: String) -> bool {
        self.set.insert(value)
    }

    pub fn srem(&mut self, value: &str) -> bool {
        self.set.remove(value)
    }

    pub fn smembers(&self) -> Vec<String> {
        self.set.iter().cloned().collect()
    }

    pub fn sismember(&self, value: &str) -> bool {
        self.set.contains(value)
    }
}

#[derive(Clone, Eq)]
pub struct SortedMember {
    pub member: String,
    pub score: OrderedFloat<f64>,
}

impl PartialEq for SortedMember {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member && self.score == other.score
    }
}

impl Ord for SortedMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PartialOrd for SortedMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct RSortedSet {
    pub members: HashMap<String, OrderedFloat<f64>>,
    pub sorted: BTreeSet<SortedMember>,
}

impl RSortedSet {
    pub fn new() -> Self {
        RSortedSet {
            members: HashMap::new(),
            sorted: BTreeSet::new(),
        }
    }

    pub fn zadd(&mut self, score: f64, member: String) -> bool {
        let score = OrderedFloat(score);
        if let Some(&old_score) = self.members.get(&member) {
            if old_score == score {
                return false; // No change
            }
            self.sorted.remove(&SortedMember {
                member: member.clone(),
                score: old_score,
            });
        }
        let new_entry = SortedMember {
            member: member.clone(),
            score,
        };
        self.members.insert(member, score);
        self.sorted.insert(new_entry)
    }

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

    pub fn zrange(&self, start: usize, end: usize) -> Vec<String> {
        self.sorted
            .iter()
            .skip(start)
            .take(end - start + 1)
            .map(|m| m.member.clone())
            .collect()
    }

    pub fn zscore(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|s| s.into_inner())
    }
}
