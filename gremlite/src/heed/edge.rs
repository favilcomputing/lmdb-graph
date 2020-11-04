use heed::{RoIter, RoRange, RoTxn, RwTxn};
use std::{fmt::Debug, marker::PhantomData, ops::Deref};

use super::{IdParam, LabelId, ParamId};
use crate::{
    error::Result,
    graph::{Edge, Id, PValue, Type, Writable},
    heed::Graph,
};

impl<V, E, P> Graph<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    pub fn put_edge(&self, txn: &mut RwTxn, edge: &Edge<V, E, P>) -> Result<Edge<V, E, P>> {
        let e = if edge.id.is_some() {
            let e: Option<_> = self.edge_db.get(txn, edge.id.as_ref().unwrap())?;
            if let Some(e) = e {
                self.edge_idx_db
                    .delete(txn, &LabelId(e.label, edge.id.unwrap()))?;
            }
            edge.clone()
        } else {
            let id = Id::new(Type::Edge, &mut self.generator.lock())?;
            Edge {
                id: Some(id),
                ..edge.clone()
            }
        };
        self.edge_db.put(txn, e.id.as_ref().unwrap(), &e)?;
        let rev = &LabelId(e.label.clone(), e.id.unwrap());
        self.edge_idx_db.put(txn, rev, e.id.as_ref().unwrap())?;
        e.parameters.iter().for_each(|(k, v)| {
            self.parameters_db
                .put(txn, &IdParam(e.id.unwrap(), k.clone()), v)
                .unwrap();
            self.parameters_idx_db
                .put(txn, &ParamId(k.clone(), e.id.unwrap()), &e.id.unwrap())
                .unwrap();
        });
        // TODO: Add Hexstore stuff for faster searching
        Ok(e)
    }

    pub fn get_edge_by_id(&self, txn: &RoTxn, id: &Id) -> Result<Option<Edge<V, E, P>>> {
        let edge = self.edge_db.get(txn, id)?;
        Ok(edge)
    }

    pub fn get_edges_by_ids<'txn, Txn>(
        &'txn self,
        txn: &'txn Txn,
        ids: Vec<Id>,
    ) -> Result<impl 'txn + Iterator<Item = PValue<V, E, P>>>
    where
        Txn: Deref<Target = RoTxn> + ?Sized,
    {
        Ok(ids
            .into_iter()
            .map(move |id| self.edge_db.get(txn, &id).ok())
            .flatten()
            .flatten()
            .map(PValue::Edge))
    }

    pub fn get_edges_by_label<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        label: &E,
    ) -> Result<EdgeRange<'txn, V, E, P>>
where {
        // let prefix: Vec<u8> = Edge::<E, P>::label_to_db(value)?;
        // let iter = self.edge_idx_db.prefix_iter(txn, &prefix)?;
        let range = LabelId(label.clone(), Id::nil(Type::Edge))
            ..LabelId(label.clone(), Id::max(Type::Edge));
        let iter: RoRange<LabelId<E>, Id> = self.edge_idx_db.range(txn, &range)?;
        Ok(EdgeRange::new(self, txn, iter))
    }

    pub fn get_edge_by_label<'txn>(
        &self,
        txn: &'txn RoTxn,
        value: &E,
    ) -> Result<Option<Edge<V, E, P>>>
    where
        E: Clone + Debug,
    {
        Ok(self.get_edges_by_label(txn, value)?.next())
    }

    pub fn edge_count(&self, txn: &RoTxn) -> Result<usize> {
        assert_eq!(self.edge_db.len(txn)?, self.edge_idx_db.len(txn)?);
        Ok(self.edge_db.len(txn)?)
    }

    pub fn edges<'txn>(
        &self,
        txn: &'txn RoTxn,
    ) -> Result<impl 'txn + Iterator<Item = PValue<V, E, P>>> {
        Ok(EdgeIter {
            iter: self.edge_db.iter(txn)?,
        })
    }
}

pub struct EdgeIter<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    pub(crate) iter: RoIter<'txn, Id, Edge<V, E, P>>,
}

impl<'txn, V, E, P> Iterator for EdgeIter<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    type Item = PValue<V, E, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(edge)) => Some(PValue::Edge(edge.1)),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

pub struct EdgeRange<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    pub(crate) iter: RoRange<'txn, LabelId<E>, Id>,
    graph: &'txn Graph<V, E, P>,
    txn: &'txn RoTxn,
    _marker: PhantomData<(E, P, V)>,
}

impl<'txn, V, E, P> Iterator for EdgeRange<'txn, V, E, P>
where
    V: 'txn + Writable,
    E: 'txn + Writable,
    P: 'txn + Writable,
{
    type Item = Edge<V, E, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(next)) => self.graph.get_edge_by_id(self.txn, &next.1).unwrap(),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

impl<'txn, V, E, P> EdgeRange<'txn, V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    pub fn new(
        graph: &'txn Graph<V, E, P>,
        txn: &'txn RoTxn,
        iter: RoRange<'txn, LabelId<E>, Id>,
    ) -> Self {
        Self {
            iter,
            graph,
            txn,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    use super::*;
    use crate::graph::{parameter::FromPValue, Vertex};

    #[allow(dead_code)]
    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String, String, ()> {
        Graph::new(tmpdir.path()).unwrap()
    }

    struct Pair(Vertex<String, String, ()>, Vertex<String, String, ()>);

    #[fixture]
    fn vertices(graph: Graph<String, String, ()>) -> Pair {
        let mut txn = graph.write_txn().unwrap();
        let ferb = graph
            .put_vertex(&mut txn, &Vertex::new("ferb".into()))
            .unwrap();
        let phineas = graph
            .put_vertex(&mut txn, &Vertex::new("phineas".into()))
            .unwrap();
        txn.commit().unwrap();
        Pair(ferb, phineas)
    }

    #[rstest]
    fn test_edge_put(graph: Graph<String, String, ()>, vertices: Pair) -> Result<()> {
        let Pair(ferb, phineas) = vertices;
        let mut txn = graph.write_txn()?;
        let edge = graph.put_edge(&mut txn, &Edge::new(&ferb, &phineas, "brothers".into())?)?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_edge_by_id(&txn, &edge.id.unwrap())?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.label, edge.label);
        Ok(())
    }

    #[rstest]
    fn test_edge_get_by_value(graph: Graph<String, String, ()>, vertices: Pair) -> Result<()> {
        let Pair(ferb, phineas) = vertices;
        let mut txn = graph.write_txn()?;
        let value: String = "brothers".into();
        let edge = graph.put_edge(&mut txn, &Edge::new(&ferb, &phineas, value.clone())?)?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_edge_by_label(&txn, &value)?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.label, edge.label);
        assert_eq!(fetched.to, edge.to);
        assert_eq!(fetched.from, edge.from);
        Ok(())
    }

    #[rstest]
    fn test_edges(graph: Graph<String, String, ()>, vertices: Pair) -> Result<()> {
        let Pair(ferb, phineas) = vertices;
        let mut txn = graph.write_txn()?;
        let mut returned = vec![];

        for i in 0..10 {
            returned.push(graph.put_edge(
                &mut txn,
                &Edge::new(&ferb, &phineas, format!("test {}", i).into())?,
            )?);
        }
        txn.commit()?;

        let txn = graph.read_txn()?;
        let edges: Vec<Edge<_, _, _>> = graph
            .edges(&txn)?
            .map(FromPValue::from_pvalue)
            .flatten()
            .collect();
        assert_eq!(edges, returned);
        Ok(())
    }

    #[rstest]
    fn test_put_existing_edge(graph: Graph<String, String, ()>, vertices: Pair) -> Result<()> {
        init();
        let Pair(ferb, phineas) = vertices;
        let mut txn = graph.write_txn()?;

        let value: String = "brothers".into();
        let edge = &Edge::new(&ferb, &phineas, value.clone())?;

        let mut returned = graph.put_edge(&mut txn, &edge.clone())?;
        returned.label = "sisters".to_string();
        graph.put_edge(&mut txn, &returned.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;

        assert_eq!(graph.edge_count(&txn)?, 1);
        let n = graph.get_edge_by_id(&txn, returned.id.as_ref().unwrap())?;
        assert!(n.is_some());
        assert_eq!(n.unwrap().label, returned.label);

        Ok(())
    }
}
