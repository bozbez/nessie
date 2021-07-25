use crate::chain::Chain;

use mongodb::Collection;

struct ChainInserter {
    collection: Collection<(Bigram, TopicMap)>,
    chain: Chain
}
