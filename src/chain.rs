use bumpalo::Bump;
use crate::counter::Counter;
use hashbrown::HashMap;
use smartstring::SmartString;

use std::{
    cell::UnsafeCell,
    cmp::min,
    mem,
    ptr::drop_in_place,
};

pub type Unigram = SmartString<smartstring::LazyCompact>;
pub type Bigram = (Unigram, Unigram);

pub type ChainMap = HashMap<Bigram, HashMap<Bigram, Vec<(i32, Unigram)>>>;

type BHashMap<'a, K, V> = HashMap<K, V, ahash::RandomState, &'a Bump>;
type BVec<'a, T> = Vec<T, &'a Bump>;

type BChainMap<'a> = BHashMap<'a, Bigram, BHashMap<'a, Bigram, BVec<'a, (i32, Unigram)>>>;

pub struct Chain<'a> {
    half_para_len: usize,
    prune_size: usize,
    prune_threshold: usize,

    chain: BChainMap<'a>,

    pools: Vec<UnsafeCell<Bump>>,
    active_pool: usize,
}

impl<'a> Chain<'a> {
    pub fn new(half_para_len: usize, prune_size: usize, prune_threshold: usize) -> Self {
        let pools = vec![
            UnsafeCell::new(Bump::with_capacity((prune_size as f64 * 1.1) as usize)),
            UnsafeCell::new(Bump::with_capacity((prune_size as f64 * 1.1) as usize)),
        ];

        Chain {
            half_para_len,
            prune_size,
            prune_threshold,

            chain: BChainMap::with_capacity_in(prune_size / 1000, unsafe {
                mem::transmute(pools[0].get())
            }),

            pools,
            active_pool: 0,
        }
    }

    fn active_pool(&self) -> &'a Bump {
        unsafe { mem::transmute(self.pools[self.active_pool].get()) }
    }

    fn advance_pool(&mut self) -> &'a Bump {
        self.active_pool = (self.active_pool + 1) % self.pools.len();
        self.active_pool()
    }

    pub fn update(&mut self, words: &[&str]) {
        if words.len() < self.half_para_len {
            return;
        }

        let pool = self.active_pool();

        let mut seq_num = 0;
        let mut previous_topic_bigram = (Unigram::from(""), Unigram::from(""));

        let mut counter = Counter::<&str>::new();

        for i in 0..(words.len() - 1) {
            let start = i.saturating_sub(self.half_para_len);
            let end = min(i.saturating_add(self.half_para_len), words.len());

            let para = &words[start..end];

            if i == 0 {
                for word in para.iter().filter(|w| w.len() > 2) {
                    counter.add(word);
                }
            } else {
                if start > 0 {
                    let word = &words[start];
                    if word.len() > 2 {
                        counter.remove(word);
                    }
                }

                if end < words.len() || i + self.half_para_len == words.len() {
                    let word = &words[end - 1];
                    if word.len() > 2 {
                        counter.add(word);
                    }
                }
            }

            if counter.total_count() < 3 || counter.num_items() < 2 {
                break;
            }

            let topic_bigram = (
                Unigram::from(counter.most_frequent(1).unwrap().0),
                Unigram::from(counter.most_frequent(2).unwrap().0),
            );

            if topic_bigram != previous_topic_bigram {
                seq_num = 0;
                previous_topic_bigram = topic_bigram.clone();
            }

            let next_unigram = if i + 2 >= words.len() {
                Unigram::from("$")
            } else {
                Unigram::from(words[i + 2])
            };

            self.chain
                .entry((Unigram::from(words[i]), Unigram::from(words[i + 1])))
                .or_insert(BHashMap::new_in(pool))
                .entry(topic_bigram)
                .or_insert(BVec::new_in(pool))
                .push((seq_num, next_unigram));

            seq_num += 1;
        }

        if self.allocated_bytes() > self.prune_size {
            self.prune();
        }
    }

    unsafe fn drop_unigram(u: &Unigram) {
        if u.is_inline() {
            return;
        }

        let cptr = u as *const Unigram;
        drop_in_place(cptr as *mut Unigram);
    }

    unsafe fn drop_bigram(b: &(Unigram, Unigram)) {
        Self::drop_unigram(&b.0);
        Self::drop_unigram(&b.1);
    }

    pub fn prune(&mut self) {
        let old_pool_id = self.active_pool;
        let new_pool = self.advance_pool();

        let new_size = (self.num_entries() as f64 * 1.4) as usize;

        let mut new_chain = BChainMap::with_capacity_in(new_size, new_pool);
        for (bigram, topic_map) in self
            .chain
            .iter()
            .filter(|(_, topic_map)| topic_map.len() >= self.prune_threshold)
        {
            let mut new_topic_map = BHashMap::with_capacity_in(topic_map.len(), new_pool);
            for (topic, unigrams) in topic_map.iter() {
                let mut new_unigrams = BVec::with_capacity_in(unigrams.len(), new_pool);

                for unigram in unigrams {
                    new_unigrams.push(unigram.clone());
                    unsafe { Self::drop_unigram(&unigram.1) }
                }

                new_topic_map.insert(topic.clone(), new_unigrams);
                unsafe { Self::drop_bigram(topic) }
            }

            new_chain.insert(bigram.clone(), new_topic_map);
            unsafe { Self::drop_bigram(bigram) }
        }

        mem::swap(&mut self.chain, &mut new_chain);
        mem::forget(new_chain);

        unsafe { self.reset_pool(old_pool_id) }
    }

    unsafe fn reset_pool(&mut self, id: usize) {
        self.pools[id].get_mut().reset();
    }

    pub fn num_entries(&self) -> usize {
        self.chain.len()
    }

    pub fn allocated_bytes(&self) -> usize {
        self.active_pool().allocated_bytes()
    }

    pub fn extract_map(&self) -> ChainMap {
        let mut new_chain = ChainMap::with_capacity(self.num_entries());
        for (bigram, topic_map) in self.chain.iter() {
            let mut new_topic_map = HashMap::with_capacity(topic_map.len());
            for (topic, unigrams) in topic_map.iter() {
                let mut new_unigrams = Vec::with_capacity(unigrams.len());
                new_unigrams.extend_from_slice(unigrams);

                new_topic_map.insert(topic.clone(), new_unigrams);
            }

            new_chain.insert(bigram.clone(), new_topic_map);
        }

        new_chain
    }
}
