use heed::{RoIter, RoRange, RoTxn, RwTxn};
use std::{clone::Clone, fmt::Debug, marker::PhantomData};

use super::{Graph, IdParam, LabelId, ParamId};
use crate::{
    error::Result,
    graph::{Id, PValue, Type, Vertex, Writable},
};

impl<V, E, P> Graph<V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    pub fn put_vertex(&self, txn: &mut RwTxn, n: &Vertex<V, E, P>) -> Result<Vertex<V, E, P>> {
        let n = if n.id.is_some() {
            let vertex: Option<Vertex<V, E, P>> = self.vertex_db.get(txn, &n.id.unwrap())?;
            if let Some(vertex) = vertex {
                self.vertex_idx_db
                    .delete(txn, &LabelId(vertex.label, vertex.id.unwrap()))?;
            }
            n.to_owned()
        } else {
            let id = Id::new(Type::Vertex, &mut self.generator.lock())?;
            Vertex {
                id: Some(id),
                ..n.clone()
            }
        };
        self.vertex_db.put(txn, n.id.as_ref().unwrap(), &n)?;
        let rev = &LabelId(n.label.clone(), n.id.unwrap());
        self.vertex_idx_db.put(txn, rev, n.id.as_ref().unwrap())?;
        n.parameters.iter().for_each(|(k, v)| {
            self.parameters_db
                .put(txn, &IdParam(n.id.unwrap(), k.clone()), v)
                .unwrap();
            self.parameters_idx_db
                .put(txn, &ParamId(k.clone(), n.id.unwrap()), &n.id.unwrap())
                .unwrap();
        });

        Ok(n)
    }

    pub fn get_vertex_by_id(&self, txn: &RoTxn, id: &Id) -> Result<Option<Vertex<V, E, P>>> {
        let vertex = self.vertex_db.get(txn, id)?;
        Ok(vertex)
    }

    pub fn get_vertices_by_ids<'graph, 'txn>(
        &'graph self,
        txn: &'txn RoTxn,
        ids: Vec<Id>,
    ) -> Result<impl 'txn + Iterator<Item = PValue<V, E, P>>>
    where
        'graph: 'txn,
    {
        Ok(ids
            .into_iter()
            .map(move |id| self.vertex_db.get(txn, &id).ok())
            .flatten()
            .flatten()
            .map(PValue::Vertex))
    }

    pub fn get_vertices_by_label<'txn>(
        &'txn self,
        txn: &'txn RoTxn,
        label: &V,
    ) -> Result<VertexRange<'txn, V, E, P>>
    where
        V: Clone + Debug,
    {
        // let prefix: Vec<u8> = Vertex::<VertexT, P>::label_to_db(label)?;
        // let iter = self.vertex_idx_db.prefix_iter(txn, &prefix)?;
        let range = LabelId(label.clone(), Id::nil(Type::Vertex))
            ..=LabelId(label.clone(), Id::max(Type::Vertex));
        let iter: RoRange<LabelId<V>, Id> = self.vertex_idx_db.range(txn, &range)?;
        Ok(VertexRange::new(self, txn, iter))
    }

    pub fn get_vertex_by_label<'txn>(
        &self,
        txn: &'txn RoTxn,
        value: &V,
    ) -> Result<Option<Vertex<V, E, P>>>
    where
        V: Clone + Debug,
    {
        Ok(self.get_vertices_by_label(txn, value)?.next())
    }

    pub fn vertex_count(&self, txn: &RoTxn) -> Result<usize>
where {
        assert_eq!(self.vertex_db.len(txn)?, self.vertex_idx_db.len(txn)?);
        Ok(self.vertex_db.len(txn)?)
    }

    pub fn vertices<'txn>(
        &self,
        txn: &'txn RoTxn,
    ) -> Result<impl 'txn + Iterator<Item = PValue<V, E, P>>> {
        Ok(VertexIter {
            iter: self.vertex_db.iter(txn)?,
        })
    }
}

pub struct VertexIter<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    pub(crate) iter: RoIter<'txn, Id, Vertex<V, E, P>>,
}

impl<'txn, V, E, P> Iterator for VertexIter<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    type Item = PValue<V, E, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(vertex)) => Some(PValue::Vertex(vertex.1)),
            Some(Err(_)) | None => None,
        }
    }
}

pub struct VertexRange<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    iter: RoRange<'txn, LabelId<V>, Id>,
    graph: &'txn Graph<V, E, P>,
    txn: &'txn RoTxn,
    _marker: PhantomData<(V, P)>,
}

impl<'txn, V, E, P> VertexRange<'txn, V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub fn new(
        graph: &'txn Graph<V, E, P>,
        txn: &'txn RoTxn,
        iter: RoRange<'txn, LabelId<V>, Id>,
    ) -> Self {
        Self {
            iter,
            graph,
            txn,
            _marker: PhantomData,
        }
    }
}

impl<'txn, V, E, P> Iterator for VertexRange<'txn, V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    type Item = Vertex<V, E, P>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(next)) => self.graph.get_vertex_by_id(self.txn, &next.1).unwrap(),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use super::*;
    use crate::graph::parameter::{PValue, ToPValue};
    use tempfile::TempDir;

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

    #[rstest]
    fn test_put(graph: Graph<String, String, ()>) -> Result<()> {
        let vertex = Vertex::new("test".to_string());
        let mut txn = graph.write_txn().unwrap();
        let returned = graph.put_vertex(&mut txn, &vertex.clone()).unwrap();
        txn.commit()?;
        assert_eq!(vertex.id, None);
        assert_ne!(returned.id, None);
        assert_eq!(returned.get_label(), vertex.get_label());

        Ok(())
    }

    #[rstest]
    fn test_get(graph: Graph<String, String, ()>) -> Result<()> {
        let vertex = Vertex::new("test".to_string());

        let mut txn = graph.write_txn()?;
        let returned = graph.put_vertex(&mut txn, &vertex.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph.get_vertex_by_id(&txn, &returned.id.unwrap())?;
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.id, returned.id);
        assert_eq!(vertex.label, fetched.label);

        let none = graph.get_vertex_by_id(&txn, &Id::nil(Type::Vertex))?;
        assert!(none.is_none());

        Ok(())
    }

    #[rstest]
    fn test_get_value(graph: Graph<String, String, ()>) -> Result<()> {
        let vertex = Vertex::new("test".to_string()).set_param((), PValue::None);

        let mut txn = graph.write_txn()?;
        let returned = graph.put_vertex(&mut txn, &vertex.clone())?;
        graph.put_vertex(&mut txn, &Vertex::new("test3".to_string()))?;
        txn.commit()?;

        let txn = graph.read_txn()?;
        let fetched = graph
            .get_vertices_by_label(&txn, &vertex.label)?
            .collect::<Vec<_>>();
        assert_eq!(fetched.len(), 1);
        let fetch = &fetched[0];
        assert_eq!(fetch.id, returned.id);
        assert_eq!(fetch.label, vertex.label);

        let fetched = graph.get_vertex_by_label(&txn, &"test2".to_string())?;
        assert!(fetched.is_none());
        Ok(())
    }

    #[rstest]
    fn test_vertex_iter(graph: Graph<String, String, ()>) -> Result<()> {
        let mut returned = vec![];
        let mut txn = graph.write_txn()?;

        for i in 0..10 {
            let vertex = Vertex::<String, String, ()>::new(format!("test {}", i).to_string());
            returned.push(graph.put_vertex(&mut txn, &vertex.clone())?.to_pvalue());
        }
        txn.commit()?;

        let txn = graph.read_txn()?;
        let vertices: Vec<_> = graph.vertices(&txn)?.collect();
        assert_eq!(vertices, returned);

        Ok(())
    }

    #[rstest]
    fn test_put_existing_vertex(graph: Graph<String, String, ()>) -> Result<()> {
        let vertex = Vertex::new("tester".to_string());
        let mut txn = graph.write_txn()?;

        let mut returned = graph.put_vertex(&mut txn, &vertex.clone())?;
        returned.label = "testers".to_string();
        graph.put_vertex(&mut txn, &returned.clone())?;
        txn.commit()?;

        let txn = graph.read_txn()?;

        assert_eq!(graph.vertex_count(&txn)?, 1);
        let n = graph.get_vertex_by_id(&txn, returned.id.as_ref().unwrap())?;
        assert!(n.is_some());
        assert_eq!(n.unwrap().label, returned.label);

        Ok(())
    }
}
