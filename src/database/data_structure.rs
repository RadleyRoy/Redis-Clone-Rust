use std::collections::VecDeque;
use std::collections::HashSet;

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