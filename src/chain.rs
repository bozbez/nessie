use crate::clone_in::CloneIn;
use crate::counter::Counter;
use crate::unigram;

use bumpalo::Bump;
use hashbrown::HashMap;

use std::{cell::UnsafeCell, cmp::min, hash::Hash, mem};

pub type Unigram = String;
pub type Bigram = (String, String);

pub type TopicMap = HashMap<Bigram, Vec<(i32, Option<Unigram>)>>;
pub type ChainMap = HashMap<Bigram, TopicMap>;

type BUnigram<'a> = unigram::Unigram<&'a Bump>;
type BBigram<'a> = (BUnigram<'a>, BUnigram<'a>);

type BHashMap<'a, K, V> = HashMap<K, V, ahash::RandomState, &'a Bump>;
type BVec<'a, T> = Vec<T, &'a Bump>;

type BTopicMap<'a> = BHashMap<'a, BBigram<'a>, BVec<'a, (i32, Option<BUnigram<'a>>)>>;
type BChainMap<'a> = BHashMap<'a, BBigram<'a>, BTopicMap<'a>>;

pub struct Chain<'a> {
    half_para_len: usize,
    prune_size: usize,
    prune_threshold: usize,

    hasher: ahash::RandomState,
    chain: BChainMap<'a>,

    pools: Vec<UnsafeCell<Bump>>,
    active_pool: usize,
}

impl<'a> Chain<'a> {
    pub fn new(half_para_len: usize, prune_size: usize, prune_threshold: usize) -> Self {
        let bump_capacity = (prune_size as f64 * 1.1) as usize;

        let pools = vec![
            UnsafeCell::new(Bump::with_capacity(bump_capacity)),
            UnsafeCell::new(Bump::with_capacity(bump_capacity)),
        ];

        let hasher = ahash::RandomState::new();

        Chain {
            half_para_len,
            prune_size,
            prune_threshold,

            hasher: hasher.clone(),
            chain: BChainMap::with_capacity_and_hasher_in(prune_size / 1000, hasher, unsafe {
                &*pools[0].get()
            }),

            pools,
            active_pool: 0,
        }
    }

    fn active_pool(&self) -> &'a Bump {
        unsafe { &*self.pools[self.active_pool].get() }
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
        let mut previous_topic_bigram = (
            BUnigram::from_slice_in("", pool),
            BUnigram::from_slice_in("", pool),
        );

        let mut counter = Counter::new();

        for i in 0..(words.len() - 1) {
            let start = i.saturating_sub(self.half_para_len);
            let end = min(i.saturating_add(self.half_para_len), words.len());

            let para = &words[start..end];

            if i == 0 {
                for &word in para.iter().filter(|w| w.len() > 2) {
                    counter.add(word);
                }
            } else {
                if start > 0 {
                    let word = words[start];
                    if word.len() > 2 {
                        counter.remove(word);
                    }
                }

                if end < words.len() || i + self.half_para_len == words.len() {
                    let word = words[end - 1];
                    if word.len() > 2 {
                        counter.add(word);
                    }
                }
            }

            if counter.total_count() < 3 || counter.num_items() < 2 {
                break;
            }

            let topic_bigram = (
                BUnigram::from_slice_in(counter.most_frequent(1).unwrap().0, pool),
                BUnigram::from_slice_in(counter.most_frequent(2).unwrap().0, pool),
            );

            if topic_bigram != previous_topic_bigram {
                seq_num = 0;
                previous_topic_bigram = topic_bigram.clone_in(pool);
            }

            let next_unigram = words.get(i + 2).map(|&w| BUnigram::from_slice_in(w, pool));

            self.chain
                .entry((
                    BUnigram::from_slice_in(words[i], pool),
                    BUnigram::from_slice_in(words[i + 1], pool),
                ))
                .or_insert(BHashMap::with_hasher_in(self.hasher.clone(), pool))
                .entry(topic_bigram)
                .or_insert(BVec::new_in(pool))
                .push((seq_num, next_unigram));

            seq_num += 1;
        }

        if self.allocated_bytes() > self.prune_size {
            self.prune();
        }
    }

    fn new_hash_map<K: Hash + Eq, V>(&self, size: usize) -> BHashMap<'a, K, V> {
        BHashMap::with_capacity_and_hasher_in(size, self.hasher.clone(), self.active_pool())
    }

    pub fn prune(&mut self) {
        let old_pool_id = self.active_pool;
        let new_pool = self.advance_pool();

        let mut new_chain = self.new_hash_map((self.num_entries() as f64 * 1.4) as usize);
        for (bigram, topic_map) in self
            .chain
            .iter()
            .filter(|(_, topic_map)| topic_map.len() >= self.prune_threshold)
        {
            let mut new_topic_map = self.new_hash_map(topic_map.len());
            for (topic, unigrams) in topic_map.iter() {
                let mut new_unigrams = BVec::with_capacity_in(unigrams.len(), new_pool);
                for unigram in unigrams {
                    let new_unigram = unigram.1.as_ref().map(|u| u.clone_in(new_pool));
                    new_unigrams.push((unigram.0, new_unigram));
                }

                new_topic_map.insert(topic.clone_in(new_pool), new_unigrams);
            }

            new_chain.insert(bigram.clone_in(new_pool), new_topic_map);
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
        for ((b1, b2), topic_map) in self.chain.iter() {
            let mut new_topic_map = HashMap::with_capacity(topic_map.len());
            for ((t0, t1), unigrams) in topic_map.iter() {
                let mut new_unigrams = Vec::with_capacity(unigrams.len());
                for (u1, u2) in unigrams {
                    new_unigrams.push((*u1, u2.as_ref().map(|u| u.into())));
                }

                new_topic_map.insert((t0.into(), t1.into()), new_unigrams);
            }

            new_chain.insert((b1.into(), b2.into()), new_topic_map);
        }

        new_chain
    }
}
