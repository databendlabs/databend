use crate::storages::Table;

trait ReadableTable {}

impl<T: Table> ReadableTable for T {}
