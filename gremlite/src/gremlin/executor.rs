use crate::{
    error::{Error, Result},
    graph::{
        parameter::{FromPValue, ToPValue},
        Edge, Id, PValue, Vertex, Writable,
    },
    gremlin::Bytecode,
    heed::Graph,
};

use super::bytecode::{self, Instruction};
use heed::RwTxn;
use std::{collections::VecDeque, marker::PhantomData};

pub struct WriteExecutor<'graph, End, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
    End: FromPValue<V, E, P>,
{
    graph: &'graph Graph<V, E, P>,
    _marker: PhantomData<(End,)>,
}

impl<'graph, End, V, E, P> WriteExecutor<'graph, End, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
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
        let head = steps.pop_front().unwrap();
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
                let v = self.graph.put_vertex(txn, &Vertex::new(label))?.to_pvalue();
                Box::new(vec![v].into_iter())
            }
            Instruction::AddE(label) => {
                let (to, from) = Self::pop_to_from(&mut steps)?;
                let e = self
                    .graph
                    .put_edge(txn, &Edge::<V, E, P>::new(to, from, label)?)?
                    .to_pvalue();
                Box::new(vec![e].into_iter())
            }
            _ => todo!(),
        };
        Ok(iter)
    }

    fn pop_to_from(steps: &mut VecDeque<Instruction<V, E, P>>) -> Result<(Id, Id)> {
        let (mut to, mut from) = (None, None);
        let mut dels = vec![];
        for (idx, step) in steps.iter().enumerate() {
            match step {
                Instruction::From(id) => {
                    from = Some(*id);
                    dels.push(idx);
                }
                Instruction::To(id) => {
                    to = Some(*id);
                    dels.push(idx);
                }
                _ => {
                    // Don't care
                }
            };
        }
        for idx in dels {
            steps.remove(idx);
        }

        Ok((
            to.ok_or(Error::BadRequest("Missing to"))?,
            from.ok_or(Error::BadRequest("Missing from"))?,
        ))
    }
}

#[allow(dead_code)]
struct VertexPValueIter<Start, End, I, V, E, P>
where
    I: Iterator<Item = Start>,
    V: Writable,
    E: Writable,
    P: Writable + Eq,
    End: FromPValue<V, E, P>,
{
    iter: Option<I>,
    _marker: PhantomData<(Start, End, V, E, P)>,
}
