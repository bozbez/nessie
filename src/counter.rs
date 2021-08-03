use hashbrown::HashMap;
use std::hash::Hash;

pub struct Counter<T: Eq + Hash + Clone> {
    items: Vec<(T, usize)>,
    indicies: HashMap<T, usize>,

    total: usize,
}

impl<T: Eq + Hash + Clone> Counter<T> {
    pub fn new() -> Self {
        Counter {
            items: Vec::new(),
            indicies: HashMap::new(),

            total: 0,
        }
    }

    pub fn total_count(&self) -> usize {
        self.total
    }

    pub fn num_items(&self) -> usize {
        self.items.len()
    }

    fn update_index(&mut self, i: usize) {
        *self.indicies.get_mut(&self.items[i].0).unwrap() = i;
    }

    fn swap(&mut self, i1: usize, i2: usize) {
        if i1 == i2 {
            return;
        }

        self.items.swap(i1, i2);

        self.update_index(i1);
        self.update_index(i2);
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
            self.items[i1] = self.items[self.items.len() - 1].clone();
            self.update_index(i1);

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
        self.items.get(n - 1)
    }
}

impl<T: Eq + Hash + Clone> Default for Counter<T> {
    fn default() -> Self {
        Self::new()
    }
}
