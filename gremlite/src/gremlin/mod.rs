pub(crate) mod bytecode;
pub(crate) mod executor;
pub(crate) mod terminator;

use crate::{
    graph::{Ids, PValue, Writable},
    gremlin::{bytecode::Bytecode, terminator::Terminator},
    heed::Graph,
};
use bytecode::Instruction;
use heed::RwTxn;
use std::fmt::Debug;
use terminator::TraversalTerminator;

pub trait TraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    fn v<'a, T>(&'a self, ids: T) -> GraphTraversal<'graph, V, E, P>
    where
        T: Into<Ids>,
        'graph: 'a;

    fn e<'a, T>(&'a self, ids: T) -> GraphTraversal<'graph, V, E, P>
    where
        T: Into<Ids>,
        'graph: 'a;

    #[allow(non_snake_case)]
    fn addV<'a>(&'a self, label: V) -> GraphTraversal<'graph, V, E, P>
    where
        'graph: 'a;
}

#[derive(Clone)]
pub struct RWTraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    graph: &'graph Graph<V, E, P>,
}

impl<'graph, V, E, P> RWTraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    pub fn new(graph: &'graph Graph<V, E, P>) -> Self {
        Self { graph }
    }
}

impl<'graph, V, E, P> TraversalSource<'graph, V, E, P> for RWTraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    fn v<'a, T>(&'a self, ids: T) -> GraphTraversal<'graph, V, E, P>
    where
        T: Into<Ids>,
        'graph: 'a,
    {
        let mut code = Bytecode::default();
        code.add_step(Instruction::Vert(bytecode::Vert(ids.into())));
        GraphTraversal::new(TraversalBuilder::new(code), self.graph.terminator())
    }

    fn e<'a, T>(&'a self, ids: T) -> GraphTraversal<'graph, V, E, P>
    where
        T: Into<Ids>,
        'graph: 'a,
    {
        let mut code = Bytecode::default();
        code.add_step(Instruction::Edge(bytecode::Edge(ids.into())));
        GraphTraversal::new(TraversalBuilder::new(code), self.graph.terminator())
    }

    fn addV<'a>(&'a self, label: V) -> GraphTraversal<'graph, V, E, P>
    where
        'graph: 'a,
    {
        let mut code = Bytecode::default();
        code.add_step(Instruction::AddV(label));
        GraphTraversal::new(TraversalBuilder::new(code), self.graph.terminator())
    }
}

pub struct GraphTraversal<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    builder: TraversalBuilder<V, E, P>,
    terminator: TraversalTerminator<'graph, V, E, P>,
}

impl<'graph, V, E, P> Debug for GraphTraversal<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'term, V, E, P> GraphTraversal<'term, V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub fn new(
        builder: TraversalBuilder<V, E, P>,
        terminator: TraversalTerminator<'term, V, E, P>,
    ) -> Self {
        Self {
            builder,
            terminator,
        }
    }

    pub fn bytecode(&self) -> &Bytecode<V, E, P> {
        self.builder.bytecode()
    }

    pub fn to_list<'a>(
        &'a self,
        txn: &mut RwTxn<'term>,
    ) -> <TraversalTerminator<'term, V, E, P> as Terminator<'term, PValue<V, E, P>, V, E, P>>::List
    where
        'term: 'a,
    {
        self.terminator.to_list(txn, self.bytecode())
    }

    pub fn next<'a>(
        &'a self,
        txn: &mut RwTxn<'term>,
    ) -> <TraversalTerminator<'term, V, E, P> as Terminator<'term, PValue<V, E, P>, V, E, P>>::Next
    {
        self.terminator.next(txn, self.bytecode())
    }
}

#[derive(Debug)]
pub struct TraversalBuilder<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub(crate) bytecode: Bytecode<V, E, P>,
}

impl<V, E, P> TraversalBuilder<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub fn new(bytecode: Bytecode<V, E, P>) -> Self {
        Self { bytecode }
    }

    pub fn bytecode(&self) -> &Bytecode<V, E, P> {
        &self.bytecode
    }
}

impl<V, E, P> Default for TraversalBuilder<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    fn default() -> Self {
        Self {
            bytecode: Default::default(),
        }
    }
}
