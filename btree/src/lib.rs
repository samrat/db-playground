#![feature(iterator_step_by)]

const NUM_KEYS : usize = 3;
const NUM_POINTERS : usize = NUM_KEYS + 1;

#[derive(Clone, Copy, Debug)]
enum Pointer {
    Address(usize),
    Val(usize),
}

#[derive(Clone)]
struct Node {
    keys: Vec<i32>,
    pointers: Vec<Pointer>,
}

struct BTree {
    nodes: Vec<Node>,
}

impl BTree {
    fn new() -> BTree {
        let root = Node {
            keys: vec![3],
            pointers: vec![Pointer::Address(1), Pointer::Address(2)],
        };

        let n1 = Node {
            keys: vec![1],
            pointers: vec![Pointer::Address(111)],
        };

        let n2 = Node {
            keys: vec![4],
            pointers: vec![Pointer::Address(444)],
        };
        
        BTree {
            nodes: vec![root,n1,n2],
        }
    }
    
    fn lookup_node(self, node: usize, k: i32) -> Option<Pointer> {
        println!("node = {}", node);
        let root = &self.nodes.clone()[node];
        let mut p = None;
        for i in 0..root.keys.len() {
            if k < root.keys[i] {
                p = Some(root.pointers[i]);
            } else if k == root.keys[i] {
                return Some(root.pointers[i])
            }
        }

        match p {
            None => None,
            Some(Pointer::Address(a)) => self.lookup_node(a, k),
            Some(Pointer::Val(v)) => p
        }
    }
}

#[cfg(test)]
mod tests {
    use BTree;
    #[test]
    fn it_works() {
        let bt = BTree::new();
        println!("{:?}", bt.lookup_node(0, 1));
        assert_eq!(2 + 2, 4);
    }
}
