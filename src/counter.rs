use rustc_hash::FxHashMap;
use std::hash::Hash;

pub struct Counter<T: Eq + Hash + Clone> {
    items: Vec<(T, usize)>,
    indicies: FxHashMap<T, usize>,

    total: usize,
}

impl<T: Eq + Hash + Clone> Counter<T> {
    pub fn new() -> Self {
        Counter {
            items: Vec::new(),
            indicies: FxHashMap::default(),

            total: 0,
        }
    }

    pub fn total_count(&self) -> usize {
        return self.total;
    }

    pub fn num_items(&self) -> usize {
        return self.items.len();
    }

    fn swap(&mut self, i1: usize, i2: usize) {
        if i1 == i2 {
            return;
        }

        self.items.swap(i1, i2);

        *self.indicies.get_mut(&self.items[i1].0).unwrap() = i1;
        *self.indicies.get_mut(&self.items[i2].0).unwrap() = i2;
    }

    pub fn add(&mut self, key: T) {
        self.total += 1;

        let entry = self.indicies.entry(key.clone()).or_insert(self.items.len());
        let i1 = *entry;

        if i1 == self.items.len() {
            self.items.push((key, 1));
            return;
        }

        self.items[i1].1 += 1;
        let count = self.items[i1].1;

        for i2 in (0..i1).rev() {
            if self.items[i2].1 < count {
                if i2 > 0 {
                    continue;
                }

                self.swap(i1, 0);
                break;
            }

            self.swap(i1, i2 + 1);
            break;
        }
    }

    pub fn remove(&mut self, key: T) {
        self.total -= 1;

        let entry = self.indicies.get_mut(&key).unwrap();
        let i1 = *entry;

        self.items[i1].1 -= 1;
        let count = self.items[i1].1;

        if count == 0 {
            self.swap(i1, self.items.len() - 1);
            self.items.remove(self.items.len() - 1);
            self.indicies.remove(&key);

            return;
        }

        for i2 in (i1 + 1)..self.items.len() {
            if self.items[i2].1 > count {
                if i2 < self.items.len() - 1 {
                    continue;
                }

                self.swap(i1, i2);
                break;
            }

            self.swap(i1, i2 - 1);
            break;
        }
    }

    pub fn most_frequent(&self, n: usize) -> Option<&(T, usize)> {
        return self.items.get(n - 1);
    }
}
