use super::{bytecode::Bytecode, executor::WriteExecutor};
use crate::{
    error::Result,
    graph::{parameter::FromPValue, Writable},
    heed::Graph,
};
use heed::RwTxn;

pub trait Terminator<'graph, End, V, E, P>
where
    End: FromPValue<V, E, P>,
    V: Writable,
    E: Writable,
    P: Writable,
{
    type List;
    type Next;
    type HasNext;
    type Iter;

    fn to_list<'txn>(
        &'graph self,
        txn: &'txn mut RwTxn<'graph>,
        traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::List
    where
        'graph: 'txn;

    // TODO: Make these work
    // fn next<'txn, Start, Term>(
    //     &self,
    //     txn: &'txn mut RwTxn<'txn>,
    //     traversal: &'txn GraphTraversal<'graph, Start, End, Term, V, E, P>,
    // ) -> Self::Next
    // where
    //     'graph: 'txn,
    //     Term: Terminator<'graph, End, V, E, P>;

    // fn has_next<'txn, Start, Term>(
    //     &self,
    //     txn: &'txn mut RwTxn<'txn>,
    //     traversal: &GraphTraversal<'graph, Start, End, Term, V, E, P>,
    // ) -> Self::HasNext
    // where
    //     'graph: 'txn,
    //     Term: Terminator<'graph, End, V, E, P>;

    // fn iter<'txn, Start, Term>(
    //     &self,
    //     txn: &'txn mut RwTxn<'txn>,
    //     traversal: &GraphTraversal<'graph, Start, End, Term, V, E, P>,
    // ) -> Self::Iter
    // where
    //     'graph: 'txn,
    //     Term: Terminator<'graph, End, V, E, P>;
}

pub struct TraversalTerminator<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    graph: &'graph Graph<V, E, P>,
}

impl<'graph, V, E, P> TraversalTerminator<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    pub fn new(graph: &'graph Graph<V, E, P>) -> Self {
        Self { graph }
    }
}

impl<'graph, End, V, E, P> Terminator<'graph, End, V, E, P> for TraversalTerminator<'graph, V, E, P>
where
    End: FromPValue<V, E, P>,
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    type List = Result<Vec<End>>;
    type Next = Result<End>;
    type HasNext = ();
    type Iter = ();

    fn to_list<'txn>(
        &'graph self,
        txn: &'txn mut RwTxn<'graph>,
        bytecode: &'txn Bytecode<V, E, P>,
    ) -> Result<Vec<End>>
    where
        'graph: 'txn,
    {
        let mut executor = WriteExecutor::<'graph, End, V, E, P>::new(self.graph);
        Ok(executor
            .execute(txn, bytecode)?
            .map(End::from_pvalue)
            .map(Result::unwrap)
            .collect())
    }

    // fn next<'txn, Start, Term>(
    //     &self,
    //     txn: &'txn mut RwTxn<'txn>,
    //     traversal: &'txn GraphTraversal<'graph, Start, End, Term, V, E, P>,
    // ) -> Result<End>
    // where
    //     Term: Terminator<'graph, End, V, E, P>,
    //     'graph: 'txn,
    // {
    //     let mut executor: WriteExecutor<'graph, End, V, E, P> = WriteExecutor::new(self.graph);
    //     let iter = executor.execute(txn, traversal.bytecode())?.next();
    //     iter.map(End::from_pvalue)
    //         .unwrap_or_else(|| Err(Error::EmptyTraversal))
    // }
    // fn has_next<'txn, Start, Term>(
    //     &self,
    //     _txn: &'txn mut RwTxn,
    //     _traversal: &GraphTraversal<'graph, Start, End, Term, V, E, P>,
    // ) -> Self::HasNext
    // where
    //     'graph: 'txn,
    //     Term: Terminator<'graph, End, V, E, P>,
    // {
    //     todo!()
    // }
    // fn iter<'txn, Start, Term>(
    //     &self,
    //     _txn: &'txn mut RwTxn,
    //     _traversal: &GraphTraversal<'graph, Start, End, Term, V, E, P>,
    // ) -> Self::Iter
    // where
    //     'graph: 'txn,
    //     Term: Terminator<'graph, End, V, E, P>,
    // {
    //     todo!()
    // }
}
