use crate::{
    error::Result,
    graph::{parameter::FromPValue, PValue, Vertex, Writable},
    gremlin::Bytecode,
    heed::Graph,
};

use super::bytecode::{self, Instruction};
use heed::RwTxn;
use std::marker::PhantomData;

pub(crate) struct WriteExecutor<'graph, End, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
    End: FromPValue<V, E, P>,
{
    graph: &'graph Graph<V, E, P>,
    _marker: PhantomData<(End,)>,
}

impl<'graph, End, V, E, P> WriteExecutor<'graph, End, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
    End: FromPValue<V, E, P>,
{
    pub(crate) fn new(graph: &'graph Graph<V, E, P>) -> Self {
        Self {
            graph,
            _marker: PhantomData,
        }
    }

    pub(crate) fn execute<'txn>(
        &mut self,
        txn: &'txn mut RwTxn,
        bytecode: &Bytecode<V, E, P>,
    ) -> Result<Box<dyn 'txn + Iterator<Item = PValue<V, E, P>>>>
    where
        'graph: 'txn,
    {
        let mut steps = bytecode.steps().clone();
        if steps.is_empty() {
            return Ok(Box::new(vec![].into_iter()));
        }
        let head = steps.pop().unwrap();
        let iter: Box<dyn Iterator<Item = PValue<V, E, P>> + 'txn> = match head {
            Instruction::Vert(bytecode::Vert(ids)) => {
                if ids.0.is_empty() {
                    Box::new(self.graph.vertices(txn).unwrap())
                } else {
                    Box::new(self.graph.get_vertices_by_ids(txn, ids.0)?)
                }
            }
            Instruction::Edge(bytecode::Edge(ids)) => {
                if ids.0.is_empty() {
                    Box::new(self.graph.edges(txn).unwrap())
                } else {
                    Box::new(self.graph.get_edges_by_ids(txn, ids.0)?)
                }
            }
            Instruction::AddV(label) => {
                self.graph.put_vertex(txn, &Vertex::new(label))?;
                todo!()
            }
            _ => todo!(),
        };
        Ok(iter)
    }
}

#[allow(dead_code)]
struct VertexPValueIter<Start, End, I, V, E, P>
where
    I: Iterator<Item = Start>,
    V: Writable,
    E: Writable,
    P: Writable,
    End: FromPValue<V, E, P>,
{
    iter: Option<I>,
    _marker: PhantomData<(Start, End, V, E, P)>,
}
