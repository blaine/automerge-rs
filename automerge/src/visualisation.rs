use fxhash::FxHasher;
use std::{borrow::Cow, collections::HashMap, hash::BuildHasherDefault};

use dot;
use rand::Rng;

#[derive(Copy, Clone, PartialEq, Hash, Eq)]
pub(crate) struct NodeId(u64);

impl Default for NodeId {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let u = rng.gen();
        NodeId(u)
    }
}

#[derive(Clone)]
pub(crate) struct Node<'a, const B: usize> {
    id: NodeId,
    children: Vec<NodeId>,
    node_type: NodeType<'a, B>,
    metadata: &'a crate::op_set::OpSetMetadata,
}

#[derive(Clone)]
pub(crate) enum NodeType<'a, const B: usize> {
    ObjRoot(crate::ObjId),
    ObjTreeNode(&'a crate::op_tree::OpTreeNode<B>)
}

#[derive(Clone)]
pub(crate) struct Edge {
    parent_id: NodeId,
    child_id: NodeId,
}

pub(crate) struct GraphVisualisation<'a, const B: usize> {
    nodes: HashMap<NodeId, Node<'a, B>>,
}

impl<'a, const B: usize> GraphVisualisation<'a, B> {
    pub(super) fn construct(
        trees: &'a HashMap<crate::types::ObjId, crate::op_tree::OpTreeInternal<B>, BuildHasherDefault<FxHasher>>,
        metadata: &'a crate::op_set::OpSetMetadata,
    ) -> GraphVisualisation<'a, B> {
        let mut nodes = HashMap::new();
        for (obj_id, tree) in trees {
            if let Some(root_node) = &tree.root_node {
                let tree_id = Self::construct_nodes(&root_node, &mut nodes, metadata);
                let obj_tree_id = NodeId::default();
                nodes.insert(obj_tree_id, Node{
                    id: obj_tree_id,
                    children: vec![tree_id],
                    node_type: NodeType::ObjRoot(obj_id.clone()),
                    metadata,
                });
            }
        }
        GraphVisualisation { nodes }
    }

    fn construct_nodes(
        node: &'a crate::op_tree::OpTreeNode<B>,
        nodes: &mut HashMap<NodeId, Node<'a, B>>,
        m: &'a crate::op_set::OpSetMetadata,
    ) -> NodeId {
        let node_id = NodeId::default();
        let mut child_ids = Vec::new();
        for child in &node.children {
            let child_id = Self::construct_nodes(child, nodes, m);
            child_ids.push(child_id);
        }
        nodes.insert(
            node_id,
            Node {
                id: node_id,
                children: child_ids,
                node_type: NodeType::ObjTreeNode(node),
                metadata: m,
            },
        );
        node_id
    }
}

impl<'a, const B: usize> dot::GraphWalk<'a, &'a Node<'a, B>, Edge> for GraphVisualisation<'a, B> {
    fn nodes(&'a self) -> dot::Nodes<'a, &'a Node<'a, B>> {
        Cow::Owned(self.nodes.values().collect::<Vec<_>>())
    }

    fn edges(&'a self) -> dot::Edges<'a, Edge> {
        let mut edges = Vec::new();
        for node in self.nodes.values() {
            for child in &node.children {
                edges.push(Edge {
                    parent_id: node.id,
                    child_id: *child,
                });
            }
        }
        Cow::Owned(edges)
    }

    fn source(&'a self, edge: &Edge) -> &'a Node<'a, B> {
        self.nodes.get(&edge.parent_id).unwrap()
    }

    fn target(&'a self, edge: &Edge) -> &'a Node<'a, B> {
        self.nodes.get(&edge.child_id).unwrap()
    }
}

impl<'a, const B: usize> dot::Labeller<'a, &'a Node<'a, B>, Edge> for GraphVisualisation<'a, B> {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("OpSet").unwrap()
    }

    fn node_id(&'a self, n: &&Node<'a, B>) -> dot::Id<'a> {
        dot::Id::new(format!("node_{}", n.id.0.to_string())).unwrap()
    }

    fn node_shape(&'a self, node: &&'a Node<'a, B>) -> Option<dot::LabelText<'a>> {
        let shape = match node.node_type {
            NodeType::ObjTreeNode(_) => dot::LabelText::label("none"),
            NodeType::ObjRoot(_) => dot::LabelText::label("ellipse"),
        };
        Some(shape)
    }

    fn node_label(&'a self, n: &&Node<'a, B>) -> dot::LabelText<'a> {
        match n.node_type {
            NodeType::ObjTreeNode(tree_node) => {
                dot::LabelText::HtmlStr(OpTable::create(tree_node, n.metadata).to_html().into())
            },
            NodeType::ObjRoot(objid) => {
                dot::LabelText::label(format!("{}@{}", objid.0.counter(), n.metadata.actors[objid.0.actor()]))
            }
        }
    }
}

struct OpTable {
    rows: Vec<OpTableRow>,
}

impl OpTable {

    fn create<'a, const B: usize>(node: &'a crate::op_tree::OpTreeNode<B>, metadata: &crate::op_set::OpSetMetadata) -> Self {
        let rows = node
            .elements
            .iter()
            .map(|e| OpTableRow::create(e, metadata))
            .collect();
        OpTable { rows }
    }

    fn to_html(&self) -> String {
        let rows = self
            .rows
            .iter()
            .map(|r| r.to_html())
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<table cellspacing=\"0\">\
            <tr><td>op</td><td>obj</td><td>prop</td><td>action</td></tr>\
            <hr/>\
            {}\
            </table>",
            rows
        )
    }
}

struct OpTableRow {
    obj_id: String,
    op_id: String,
    prop: String,
    op_description: String,
}

impl OpTableRow {
    fn to_html(&self) -> String {
        format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            self.op_id, self.obj_id, self.prop, self.op_description
        )
    }
}

impl OpTableRow {
    fn create(op: &super::Op, metadata: &crate::op_set::OpSetMetadata) -> Self {
        let op_description = match &op.action {
            crate::OpType::Del => "del".to_string(),
            crate::OpType::Set(v) => format!("set {}", v),
            crate::OpType::Make(obj) => format!("make {}", obj),
            crate::OpType::Inc(v) => format!("inc {}", v),
        };
        let prop = match op.key {
            crate::Key::Map(k) => metadata.props[k].clone(),
            crate::Key::Seq(e) => format!("{}@{}", e.0.counter(), e.0.actor()),
        };
        OpTableRow {
            op_description,
            obj_id: format!("{}@{}", op.obj.0.counter(), op.obj.0.actor()),
            op_id: format!("{}@{}", op.id.counter(), op.id.actor()),
            prop,
        }
    }
}

