pub(crate) mod bytecode;
pub(crate) mod executor;
pub(crate) mod terminator;

use crate::{
    graph::{parameter::FromPValue, Ids, PValue, Vertex, Writable},
    gremlin::{bytecode::Bytecode, terminator::Terminator},
    heed::Graph,
};
use bytecode::Instruction;
use heed::RwTxn;
use std::{fmt::Debug, marker::PhantomData};
use terminator::TraversalTerminator;

pub trait TraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    fn v<'a, T>(&'a self, ids: T) -> GraphTraversal<'graph, V, E, P>
    where
        T: Into<Ids>,
        'graph: 'a;

    fn e<'a, T>(&'a self, ids: T) -> GraphTraversal<'graph, V, E, P>
    where
        T: Into<Ids>,
        'graph: 'a;
}

#[derive(Clone)]
pub struct GraphTraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    graph: &'graph Graph<V, E, P>,
}

impl<'graph, V, E, P> GraphTraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    pub fn new(graph: &'graph Graph<V, E, P>) -> Self {
        Self { graph }
    }
}

impl<'graph, V, E, P> TraversalSource<'graph, V, E, P> for GraphTraversalSource<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
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
}

pub struct GraphTraversal<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    builder: TraversalBuilder<V, E, P>,
    terminator: TraversalTerminator<'graph, V, E, P>,
}

impl<'graph, V, E, P> Debug for GraphTraversal<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'term, V, E, P> GraphTraversal<'term, V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
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

    pub fn to_list(
        &'term self,
        txn: &mut RwTxn<'term>,
    ) -> <TraversalTerminator<'term, V, E, P> as Terminator<'term, PValue<V, E, P>, V, E, P>>::List
    {
        self.terminator.to_list(txn, self.bytecode())
    }
}

#[derive(Debug)]
pub struct TraversalBuilder<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    pub(crate) bytecode: Bytecode<V, E, P>,
}

impl<V, E, P> TraversalBuilder<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
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
    P: Writable,
{
    fn default() -> Self {
        Self {
            bytecode: Default::default(),
        }
    }
}
