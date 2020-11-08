use crate::graph::{Id, Ids, PValue, Writable};
use std::collections::VecDeque;

#[derive(Debug, PartialEq, Clone)]
pub struct Bytecode<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    sources: VecDeque<Instruction<V, E, P>>,
    steps: VecDeque<Instruction<V, E, P>>,
}

impl<V, E, P> Default for Bytecode<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    fn default() -> Self {
        Self {
            sources: VecDeque::new(),
            steps: VecDeque::new(),
        }
    }
}

impl<V, E, P> Bytecode<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    // TODO: Uncomment this when it is necessary again
    // pub fn add_source(&mut self, i: Instruction<V, E, P>) {
    //     self.sources.push(i);
    // }

    pub fn add_step(&mut self, i: Instruction<V, E, P>) {
        self.steps.push_back(i);
    }

    pub fn steps(&self) -> &VecDeque<Instruction<V, E, P>> {
        &self.steps
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Vert(pub(crate) Ids);

#[derive(Debug, PartialEq, Clone)]
pub struct Edge(pub(crate) Ids);

#[derive(Debug, PartialEq, Clone)]
pub enum Instruction<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    Vert(Vert),
    Edge(Edge),
    AddV(V),
    AddE(E),
    Property(P, PValue<V, E, P>),
    From(Id),
    To(Id),
}
