use kaspa_hashes::{Hash, HasherBase, MerkleBranchHash, ZERO_HASH};

pub fn calc_merkle_root(hashes: impl ExactSizeIterator<Item = Hash>) -> Hash {
    if hashes.len() == 0 {
        return ZERO_HASH;
    }
    let next_pot = hashes.len().next_power_of_two();
    let vec_len = 2 * next_pot - 1;
    let mut merkles = vec![None; vec_len];
    for (i, hash) in hashes.enumerate() {
        merkles[i] = Some(hash);
    }
    for (parent_index, left_child_index) in (next_pot..).zip((0..vec_len - 1).step_by(2)) {
        if merkles[left_child_index].is_none() {
            merkles[parent_index] = None;
        } else {
            merkles[parent_index] =
                Some(merkle_hash(merkles[left_child_index].unwrap(), merkles[left_child_index + 1].unwrap_or(ZERO_HASH)));
        }
    }
    merkles.last().unwrap().unwrap()
}

pub fn merkle_hash(left: Hash, right: Hash) -> Hash {
    let mut hasher = MerkleBranchHash::new();
    hasher.update(left).update(right);
    hasher.finalize()
}
