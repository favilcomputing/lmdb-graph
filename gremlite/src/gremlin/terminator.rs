use super::{bytecode::Bytecode, executor::WriteExecutor};
use crate::{
    error::{Error, Result},
    graph::{parameter::FromPValue, Writable},
    heed::Graph,
};
use heed::RwTxn;

pub trait Terminator<'graph, End, V, E, P>
where
    End: FromPValue<V, E, P>,
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    type List;
    type Next;
    type HasNext;
    type Iter;

    fn to_list<'a, 'txn>(
        &'a self,
        txn: &'txn mut RwTxn<'graph>,
        traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::List
    where
        'txn: 'a;

    fn next<'a, 'txn>(
        &'a self,
        txn: &'txn mut RwTxn<'graph>,
        traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::Next
    where
        'txn: 'a;

    fn has_next<'a, 'txn>(
        &'a self,
        txn: &'txn mut RwTxn<'graph>,
        traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::HasNext
    where
        'txn: 'a;

    fn iter<'a, 'txn>(
        &'a self,
        txn: &'txn mut RwTxn<'graph>,
        traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::Iter
    where
        'txn: 'a;
}

pub struct TraversalTerminator<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    graph: &'graph Graph<V, E, P>,
}

impl<'graph, V, E, P> TraversalTerminator<'graph, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
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
    P: 'static + Writable + Eq,
{
    type List = Result<Vec<End>>;
    type Next = Result<End>;
    type HasNext = ();
    type Iter = ();

    fn to_list<'a, 'txn>(
        &'a self,
        txn: &'txn mut RwTxn<'graph>,
        bytecode: &'txn Bytecode<V, E, P>,
    ) -> Result<Vec<End>>
    where
        'txn: 'a,
    {
        let mut executor = WriteExecutor::<'graph, End, V, E, P>::new(self.graph);
        Ok(executor
            .execute(txn, bytecode)?
            .map(End::from_pvalue)
            .map(Result::unwrap)
            .collect())
    }

    fn next<'a, 'txn>(
        &'a self,
        txn: &'txn mut RwTxn<'graph>,
        traversal: &'txn Bytecode<V, E, P>,
    ) -> Result<End>
    where
        'txn: 'a,
    {
        let mut executor: WriteExecutor<End, V, E, P> = WriteExecutor::new(self.graph);
        let iter = executor.execute(txn, traversal)?.next();
        iter.map(End::from_pvalue)
            .unwrap_or_else(|| Err(Error::EmptyTraversal))
    }

    fn has_next<'a, 'txn>(
        &'a self,
        _txn: &'txn mut RwTxn<'graph>,
        _traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::HasNext
    where
        'txn: 'a,
    {
        todo!()
        // let mut executor: WriteExecutor<'graph, End, V, E, P> = WriteExecutor::new(self.graph);
        // let iter = executor.execute(txn, traversal)?.next();
    }

    fn iter<'a, 'txn>(
        &'a self,
        _txn: &'txn mut RwTxn<'graph>,
        _traversal: &'txn Bytecode<V, E, P>,
    ) -> Self::Iter
    where
        'txn: 'a,
    {
        todo!()
    }
}
