use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::rc::Rc;

use crate::common::*;
use crate::kv_trait::AuthenticatedKV;
use bitvec::prelude::*;
use serde::{
    ser::{SerializeStruct, SerializeTuple},
    Serialize, Serializer,
};

pub type NodeWrapper = Rc<RefCell<Node>>;

#[derive(Debug, Clone)]
pub struct SparseMerkleTree {
    pub root_digest: crate::common::Digest,
    pub root_node: NodeWrapper,
}

impl Default for SparseMerkleTree {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseMerkleTree {
    pub fn new() -> Self {
        let root_node = Node::new_empty_rc();
        let root_digest = root_node.borrow().get_hash();
        SparseMerkleTree {
            root_digest,
            root_node,
        }
    }
}

/// NOTE: Serialization is only for debugging purposes (i.e. to visualize the tree)
///
/// P.S: If you want to visualize the tree, you can use JSON output from the following
/// example code:
///
/// ```
/// use crate::ads::sparse_merkle_tree::SparseMerkleTree;
/// use crate::ads::kv_trait::AuthenticatedKV;
///
/// let mut smt = SparseMerkleTree::new();
/// smt = smt.insert("0".to_string(), "a".to_string());
/// smt = smt.insert("1".to_string(), "b".to_string());
/// smt = smt.insert("3".to_string(), "c".to_string());
/// smt = smt.insert("4".to_string(), "d".to_string());
/// let json_output = serde_json::json!(&smt);
/// println!("{}", json_output);
/// ```
/// Then open your browser and navigate to https://jsoncrack.com/editor.
/// Paste the JSON contents into the area on the left layout, and press
/// `Ctrl + Shift + D` to rotate the layout to simulate a tree structure.
///
/// See `example_sparse_tree_visualization.png` for an example of the output.
impl Serialize for SparseMerkleTree {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Deref from NodeWrapper to Node
        let inner_node_ref = &*self.root_node.borrow();

        // Serialize the inner Node
        inner_node_ref.serialize(serializer)
    }
}

#[derive(Debug, Clone)]
pub struct Leaf {
    pub key: String,
    pub value: String,
}

// NOTE: Serialization is only for debugging purposes (i.e. to visualize the tree)
impl Serialize for Leaf {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut struct_serializer = serializer.serialize_struct("Leaf", 3)?;
        let binary_key: Vec<_> = self
            .get_key_bits()
            .into_iter()
            .map(|v| if v { 1 } else { 0 })
            .collect();

        // Convert array to string
        let binary_key: String =
            binary_key.iter().map(|v| v.to_string()).collect();

        let binary_key = format!(
            "{}...{}",
            &binary_key[0..8],
            &binary_key[binary_key.len() - 8..]
        );

        let hash_value = hex::encode(self.get_key_digest().as_ref());

        let hash_value = format!(
            "{}...{}",
            &hash_value[0..8],
            &hash_value[hash_value.len() - 8..]
        );

        struct_serializer.serialize_field("binary_key", &binary_key)?;
        struct_serializer.serialize_field("hash", &hash_value)?;
        struct_serializer.serialize_field("key", &self.key)?;
        struct_serializer.serialize_field("value", &self.value)?;
        struct_serializer.end()
    }
}

impl Leaf {
    pub fn get_key_digest(&self) -> crate::common::Digest {
        smt_utils::get_digest(self.key.as_bytes())
    }

    pub fn get_key_bits(&self) -> BitVec<u8, Lsb0> {
        BitVec::from_slice(self.get_key_digest().as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct Branch {
    pub left: NodeWrapper,
    pub right: NodeWrapper,
    pub hash: crate::common::Digest,
}

// NOTE: Serialization is only for debugging purposes (i.e. to visualize the tree)
impl Serialize for Branch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut tuple_serializer = serializer.serialize_tuple(2)?;

        // Serialize left node
        let left = self.left.borrow().clone();
        tuple_serializer.serialize_element(&left)?;

        // Serialize right node
        let right = self.right.borrow().clone();
        tuple_serializer.serialize_element(&right)?;

        tuple_serializer.end()
    }
}

impl Branch {
    pub fn new_branch(left: NodeWrapper, right: NodeWrapper) -> Branch {
        let left_value_digest = left.borrow().get_hash();
        let right_value_digest = right.borrow().get_hash();
        let hash =
            smt_utils::hash_branch(left_value_digest, right_value_digest);
        Branch { left, right, hash }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub enum Node {
    #[default]
    Empty,
    Leaf(Leaf),
    Branch(Branch),
}

impl Node {
    pub fn get_as_leaf_node(&self) -> Option<Leaf> {
        match self {
            Node::Leaf(leaf) => Some(leaf.clone()),
            _ => None,
        }
    }

    pub fn new_empty() -> Node {
        Node::Empty
    }

    pub fn new_leaf(key: String, value: String) -> Node {
        Node::Leaf(Leaf { key, value })
    }

    pub fn new_branch(left: NodeWrapper, right: NodeWrapper) -> Node {
        Node::Branch(Branch::new_branch(left, right))
    }

    pub fn new_leaf_rc(key: String, value: String) -> NodeWrapper {
        Rc::new(RefCell::new(Node::new_leaf(key, value)))
    }

    pub fn new_branch_rc(left: NodeWrapper, right: NodeWrapper) -> NodeWrapper {
        Rc::new(RefCell::new(Node::new_branch(left, right)))
    }

    pub fn new_empty_rc() -> NodeWrapper {
        Rc::new(RefCell::new(Node::new_empty()))
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Node::Empty)
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    pub fn is_branch(&self) -> bool {
        matches!(self, Node::Branch(_))
    }

    pub fn has_non_empty_children(&self) -> bool {
        match self {
            Node::Branch(Branch { left, right, .. }) => {
                !left.borrow().is_empty() && !right.borrow().is_empty()
            }
            _ => false,
        }
    }

    pub fn get_hash(&self) -> crate::common::Digest {
        match self {
            Node::Leaf(Leaf { key, value }) => smt_utils::hash_kv(key, value),
            Node::Branch(Branch { hash, .. }) => *hash,
            Node::Empty => zero_digest(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SparseMerkleTreeProof {
    pub sibling_hashes: Vec<crate::common::Digest>,
}

impl AuthenticatedKV for SparseMerkleTree {
    type K = String;
    type V = String;
    type LookupProof = SparseMerkleTreeProof;
    type Commitment = crate::common::Digest;

    fn new() -> Self {
        Self {
            root_digest: zero_digest(),
            root_node: Default::default(),
        }
    }

    fn commit(&self) -> Self::Commitment {
        self.root_digest
    }

    fn check_proof(
        key: Self::K,
        res: Option<Self::V>,
        pf: &Self::LookupProof,
        comm: &Self::Commitment,
    ) -> Option<()> {
        // Starting root hash (leaf we are working from)
        let mut root_hash = smt_utils::hash_kv(&key, &res.unwrap_or_default());

        // Key represented as a binary (boolean) array
        let binary_key: BitVec<u8, Lsb0> =
            BitVec::from_slice(smt_utils::get_digest(&key).as_ref());

        // Length of sibling hashes, which is also the height we need to work from
        let sibling_hashes_len = pf.sibling_hashes.len();

        // Iterate over the sibling hashes and compute the root hash
        //
        // If the Merkle Root is recalculated correctly using the provided
        // proof, it confirms the valid inclusion of the data.
        pf.sibling_hashes
            .iter()
            .enumerate()
            .for_each(|(idx, sibling_hash)| {
                // Since we are working with a sparse Merkle Tree, we
                // are able to use the binary key once again to determine
                // if we have right or left siblings as we traverse up.
                root_hash = if binary_key[sibling_hashes_len - idx - 1] {
                    // We have a left sibling
                    smt_utils::hash_branch(*sibling_hash, root_hash)
                } else {
                    // We have a right sibling
                    smt_utils::hash_branch(root_hash, *sibling_hash)
                };
            });

        // Check calculated root against actual root
        match &root_hash == comm {
            true => Some(()),
            false => None,
        }
    }

    fn get(&self, key: Self::K) -> (Option<Self::V>, Self::LookupProof) {
        // Key represented as a binary (boolean) array
        let binary_key: BitVec<u8, Lsb0> =
            BitVec::from_slice(smt_utils::get_digest(&key).as_ref());

        // The current node as we traverse through the tree
        let mut current_node = self.root_node.clone();
        let mut current_node_is_branch = current_node.borrow().is_branch();

        // Sibling hashes gathered as we traverse through the tree
        let mut sibling_hashes: Vec<crate::common::Digest> = vec![];

        // Current height of traversal within the tree
        let mut height = 0;

        // We traverse through the structure starting from the top, until we find a non-branch node
        while current_node_is_branch {
            // Get the left and right children of the current node
            let (left_child, right_child) = match current_node.borrow().clone()
            {
                Node::Branch(Branch { left, right, .. }) => (left, right),
                _ => panic!("current node is not a branch"),
            };

            // Get the sibling of the current node
            let current_node_sibling = if !binary_key[height] {
                // Branch to left child
                current_node = left_child;
                // Right child is sibling
                right_child
            } else {
                // Branch to right child
                current_node = right_child;
                // Left child is sibling
                left_child
            };

            // Append sibling hash to sibling hashes
            sibling_hashes.push(current_node_sibling.borrow().get_hash());
            // Determine we've moved on to another branch, or a leaf/empty node
            current_node_is_branch = current_node.borrow().is_branch();
            height += 1;
        }

        // Reverse sibling hashes since the proof validation will need
        // to traverse the other way (up) during verification
        sibling_hashes.reverse();

        let current_node_cloned = current_node.borrow().clone();
        match current_node_cloned {
            // Node associated with target key is an empty node
            Node::Empty => (None, SparseMerkleTreeProof { sibling_hashes }),
            Node::Leaf(current_node_leaf) => {
                if current_node_leaf.key != key {
                    // Reached a leaf node, but it not associated with the target key
                    (None, SparseMerkleTreeProof { sibling_hashes })
                } else {
                    // Reached a leaf node, and it *is* associated with the target key
                    (
                        Some(current_node_leaf.value.clone()),
                        SparseMerkleTreeProof { sibling_hashes },
                    )
                }
            }
            Node::Branch(_) => panic!("current node is branch"),
        }
    }

    fn insert(self, key: Self::K, value: Self::V) -> Self {
        // Key represented as a binary (boolean) array
        let binary_key: BitVec<u8, Lsb0> =
            BitVec::from_slice(smt_utils::get_digest(&key).as_ref());

        // The new leaf that needs to get added
        let mut new_leaf = Node::new_leaf_rc(key.clone(), value);

        // Nodes that we pass through during traversal
        let mut ancestor_nodes = Vec::new();

        // The current node as we traverse through the tree (starting from root node)
        let mut current_node = self.root_node.clone();
        let mut current_node_is_branch = current_node.borrow().is_branch();

        // Current height of traversal within the tree
        let mut height = 0;

        // We move through the structure starting
        // from the top, until we find a non-branch node
        while current_node_is_branch {
            // Add our current node as an ancestor node
            ancestor_nodes.push(current_node.clone());

            // Get left and right child nodes for the branch
            let (left_child, right_child) = match current_node.borrow().clone()
            {
                Node::Branch(Branch { left, right, .. }) => (left, right),
                _ => panic!("current node is not a branch"),
            };

            current_node = if !binary_key[height] {
                left_child // Branch left on 0 bit
            } else {
                right_child // Branch right on 1 bit
            };

            // Determine we've moved on to another branch, or a leaf/empty node
            current_node_is_branch = current_node.borrow().is_branch();
            height += 1;
        }

        // Current node is a leaf node. If `get_as_leaf_node` is `None`, at this point
        // it means we have reached an empty node position, and we need to create a new leaf.
        // This means there's no need for branching.
        if let Some(current_node_leaf) =
            current_node.borrow().get_as_leaf_node()
        {
            // If `current_node_leaf.key == key` it means we are updating an existing leaf node.
            // However for `current_node_leaf.key != key` it means that we've encountered another
            // leaf at this position, and we'll need to create a new branch node, thus fufilling
            // the condition of one leaf per empty subtree.
            if current_node_leaf.key != key {
                let mut d = binary_key[height];
                let mut t = current_node_leaf.get_key_bits()[height];
                while d == t {
                    let default_branch = Node::new_branch_rc(
                        Node::new_empty_rc(),
                        Node::new_empty_rc(),
                    );
                    ancestor_nodes.push(default_branch);
                    height += 1;
                    d = binary_key[height];
                    t = current_node_leaf.get_key_bits()[height];
                }
                // Create branch node, parent of `current_node` and `new_leaf`
                if !d {
                    // We create the new leaf node as a left child of the branch
                    new_leaf = Node::new_branch_rc(
                        new_leaf,
                        Rc::new(RefCell::new(Node::Leaf(current_node_leaf))),
                    );
                } else {
                    // We create the new leaf node as a right child of the branch
                    new_leaf = Node::new_branch_rc(
                        Rc::new(RefCell::new(Node::Leaf(current_node_leaf))),
                        new_leaf,
                    );
                }
            }
        }

        // We update all branch nodes in ancestor_nodes, starting
        // from the last (traverse up again) this way we re-calculate
        // the correct `commitment` / root digest using `new_leaf` as
        // a starting point.
        let mut new_root = new_leaf.clone();
        while height > 0 {
            let d = binary_key[height - 1];
            let p = ancestor_nodes
                .get_mut(height - 1)
                .expect("cannot find node");

            let (left_sibling_node, right_sibling_node) =
                match p.borrow().clone() {
                    Node::Branch(Branch { left, right, .. }) => (left, right),
                    _ => panic!("current node is not a branch"),
                };

            if !d {
                *p = Node::new_branch_rc(new_root.clone(), right_sibling_node);
            } else {
                *p = Node::new_branch_rc(left_sibling_node, new_root.clone());
            }

            new_root = p.clone();
            height -= 1;
        }

        let new_root_digest = new_root.borrow().get_hash();
        Self {
            root_digest: new_root_digest,
            root_node: new_root,
        }
    }

    fn remove(self, key: Self::K) -> Self {
        // Key represented as a binary (boolean) array
        let binary_key: BitVec<u8, Lsb0> =
            BitVec::from_slice(smt_utils::get_digest(&key).as_ref());

        // Nodes that we pass through during traversal
        let mut ancestor_nodes = Vec::new();

        // The current node as we traverse through the tree (starting from root node)
        let mut current_node = self.root_node.clone();
        let mut current_node_is_branch = current_node.borrow().is_branch();

        // Sibling of the current node as we traverse through the tree
        let mut current_node_sibling = Node::new_empty_rc();

        // Represents the node where latest operation has been performed.
        //
        // This will also be used as a starting point during recalculation
        // of the root digest / `commitment`.
        let mut latest_node = Node::new_empty_rc();

        // Current height of traversal within the tree
        let mut height = 0;

        // We traverse through the structure starting from the top, until we find a non-branch node
        while current_node_is_branch {
            // Add our current node as an ancestor node
            ancestor_nodes.push(current_node.clone());

            // Get left and right child nodes for the branch
            let (left_child, right_child) = match current_node.borrow().clone()
            {
                Node::Branch(Branch { left, right, .. }) => (left, right),
                _ => panic!("current node is not a branch"),
            };

            if !binary_key[height] {
                // We branch left
                current_node = left_child;
                // We update the sibling of our current node
                current_node_sibling = right_child;
            } else {
                // We branch right
                current_node = right_child;
                // We update the sibling of our current node
                current_node_sibling = left_child;
            }
            // Determine we've moved on to another branch, or a leaf/empty node
            current_node_is_branch = current_node.borrow().is_branch();
            height += 1;
        }

        // Here we've either reached a leaf or empty node
        match current_node.borrow().clone() {
            // Leaf at key is already empty, so nothing to do here
            Node::Empty => return self,
            // Leaf at key is a leaf, check if associated with target key
            Node::Leaf(current_node_leaf) => {
                if current_node_leaf.key != key {
                    // Leaf at node position is not associated
                    // with target key, so nothing to do here
                    return self;
                }
            }
            Node::Branch(_) => panic!("current node is a branch"),
        };

        // Now `current_node must` be a leaf node at a position associated with the target key.
        //
        // The next step is to figure out what we do with its sibling during removal.
        let current_node_sibling_cloned = current_node_sibling.borrow().clone();
        match current_node_sibling_cloned {
            // Sibling node is empty, so we don't need to do anything with it
            Node::Empty => {}
            // Current node has a leaf sibling, which means it should move up the tree
            Node::Leaf(_) => {
                // Replace current node with an empty leaf
                *current_node.borrow_mut() = Node::new_empty_rc();
                latest_node = current_node_sibling;
                height -= 1;
                while height > 0 {
                    let p = ancestor_nodes
                        .get_mut(height - 1)
                        .expect("node not found");

                    if p.borrow().has_non_empty_children() {
                        break;
                    }

                    *p.borrow_mut() = Node::new_empty_rc();
                    ancestor_nodes.remove(height - 1);
                    height -= 1;
                }
            }
            // Current node has a branch sibling, which means it should not move
            // up in the tree, but instead we should just replace current
            // node with an empty node which would serve as a sibling to the branch.
            Node::Branch(_) => {
                *current_node.borrow_mut() = Node::new_empty_rc();
                *latest_node.borrow_mut() = Node::new_empty_rc();
            }
        };

        // We update all branch nodes in ancestor_nodes, starting
        // from the last point of operation (traverse up again),
        // this way we re-calculate the correct `commitment` / root digest
        // using `latest_node` as a starting point.
        //
        // The height is now currently at the height the removal has taken place
        while height > 0 {
            let d = binary_key[height - 1];
            let p = ancestor_nodes.get_mut(height - 1).expect("node not found");

            let (left_sibling_node, right_sibling_node) =
                match p.borrow().clone() {
                    Node::Branch(Branch { left, right, .. }) => (left, right),
                    _ => panic!("current node is not a branch"),
                };

            if !d {
                *p.borrow_mut() = Node::new_branch_rc(
                    latest_node.clone(),
                    right_sibling_node,
                );
            } else {
                *p.borrow_mut() =
                    Node::new_branch_rc(left_sibling_node, latest_node.clone());
            }

            latest_node = p.clone();
            height -= 1;
        }

        let latest_node_digest = latest_node.borrow().get_hash();
        Self {
            root_digest: latest_node_digest,
            root_node: latest_node,
        }
    }
}

mod smt_utils {
    use digest::Digest;
    use sha2::Sha256;

    pub fn hash_kv(k: &str, v: &str) -> crate::common::Digest {
        crate::common::hash_two_things("hash_kv_K", "hash_kv_V", k, v)
    }

    pub fn hash_branch(
        l: crate::common::Digest,
        r: crate::common::Digest,
    ) -> crate::common::Digest {
        crate::common::hash_two_things("hash_branch_L", "hash_branch_R", l, r)
    }

    pub fn get_digest<T: AsRef<[u8]>>(value: T) -> crate::common::Digest {
        let mut hasher = Sha256::new();
        hasher.update(value.as_ref());
        hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;

    use crate::{
        kv_trait::AuthenticatedKV,
        sparse_merkle_tree::{smt_utils::hash_kv, SparseMerkleTree},
    };

    #[test]
    fn insert_root_value() {
        let new_smt = SparseMerkleTree::new();
        let smt = new_smt.insert("key".to_string(), "value".to_string());
        assert_eq!(smt.root_digest, hash_kv("key", "value"));
    }

    #[test]
    fn test_insertion_and_removal_digest() {
        // Arrange test
        let arr = vec![
            ("0", "a"),
            ("1", "b"),
            ("2", "c"),
            ("3", "d"),
            ("4", "e"),
            ("6", "f"),
            ("7", "g"),
            ("8", "h"),
            ("9", "i"),
            ("10", "j"),
        ];

        // Choose random elements to remove
        let keys_to_truncate: Vec<&str> = arr
            .choose_multiple(&mut rand::thread_rng(), arr.len() / 2)
            .map(|(k, _)| *k)
            .collect();

        // Create sparse merkle tree
        let mut full_smt = SparseMerkleTree::new();

        // Create smt variant without all keys (lacks `keys_to_truncate`)
        let mut partial_smt = SparseMerkleTree::new();

        for (k, v) in arr.clone() {
            // Contains all keys, even `keys_to_truncate`
            full_smt = full_smt.insert(k.to_string(), v.to_string());
            // If key is present in `keys_to_truncate` then do not add.
            if !keys_to_truncate.contains(&k) {
                partial_smt = partial_smt.insert(k.to_string(), v.to_string());
            }
        }

        // Remove elements from modified_smt
        let mut modified_smt = full_smt.clone();
        for k in keys_to_truncate {
            modified_smt = modified_smt.remove(k.to_string());
        }

        // We assert that the partially constructed tree
        // as well as the tree which has had elements removed
        // result in the same root_digest.
        //
        // This is because they are supposed to reflect the exact same tree structure.
        assert_eq!(partial_smt.root_digest, modified_smt.root_digest);
    }

    #[test]
    fn test_insertion_get_and_proof_verification() {
        // Arrange test
        let existing_elements = vec![
            ("0", "a"),
            ("1", "b"),
            ("2", "c"),
            ("3", "d"),
            ("4", "e"),
            ("5", "f"),
            ("6", "g"),
            ("7", "h"),
            ("8", "i"),
            ("9", "j"),
            ("10", "k"),
            ("12", "l"),
            ("13", "m"),
        ];

        let non_existing_elements = vec![
            ("14", "n"),
            ("15", "o"),
            ("16", "p"),
            ("17", "q"),
            ("18", "r"),
            ("19", "s"),
            ("20", "t"),
            ("21", "u"),
            ("22", "v"),
            ("23", "w"),
            ("24", "x"),
            ("25", "y"),
            ("26", "z"),
        ];

        // Create sparse merkle tree
        let mut smt = SparseMerkleTree::new();

        // Insert elements
        for (k, v) in existing_elements.iter() {
            smt = smt.insert(k.to_string(), v.to_string());
        }

        // Get existing elements and check inclusive-proof (success)
        for (k, v) in existing_elements.iter() {
            let (value, proof) = smt.get(k.to_string());
            assert_eq!(value, Some(v.to_string()));
            assert!(SparseMerkleTree::check_proof(
                k.to_string(),
                value.clone(),
                &proof,
                &smt.commit(),
            )
            .is_some());
        }

        // Get non-existing elements and check inclusive-proof (failure)
        for (k, _) in non_existing_elements.iter() {
            let (value, proof) = smt.get(k.to_string());
            assert_eq!(value, None);
            assert!(SparseMerkleTree::check_proof(
                k.to_string(),
                value.clone(),
                &proof,
                &smt.commit()
            )
            .is_none());
        }
    }
}
