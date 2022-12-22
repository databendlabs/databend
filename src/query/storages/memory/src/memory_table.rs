                let mut part_end = (part + 1) * part_size;
                if part == (workers - 1) && part_remain > 0 {
                    part_end += part_remain;
                }

                partitions.push(MemoryPartInfo::create(part_begin, part_end, total));
            }
        }

        Partitions::create(PartitionsShuffleKind::Seq, partitions)
    }
}

#[async_trait::async_trait]
impl Table for MemoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        Some(self.data_metrics.clone())
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let blocks = self.blocks.read();

        let statistics = match push_downs {
            Some(push_downs) => {
                let projection_filter: Box<dyn Fn(usize) -> bool> = match push_downs.projection {
                    Some(prj) => {
                        let col_ids = match prj {
                            Projection::Columns(indices) => indices,
                            Projection::InnerColumns(path_indices) => {
                                path_indices.values().map(|i| i[0]).collect()
                            }
                        };
                        let proj_cols = HashSet::<usize>::from_iter(col_ids);
                        Box::new(move |column_id: usize| proj_cols.contains(&column_id))
                    }
                    None => Box::new(|_: usize| true),
                };

                blocks
                    .iter()
                    .fold(PartStatistics::default(), |mut stats, block| {
                        stats.read_rows += block.num_rows();
                        stats.read_bytes += (0..block.num_columns())
                            .into_iter()
                            .collect::<Vec<usize>>()
                            .iter()
                            .filter(|cid| projection_filter(**cid))
                            .map(|cid| {
                                let (val, _) = &block.columns()[*cid];
                                val.as_ref().memory_size() as u64
                            })
                            .sum::<u64>() as usize;

                        stats
                    })
            }
            None => {
                let rows = blocks.iter().map(|block| block.num_rows()).sum();
                let bytes = blocks.iter().map(|block| block.memory_size()).sum();

                PartStatistics::new_exact(rows, bytes, blocks.len(), blocks.len())
            }
        };

        let parts = Self::generate_memory_parts(
            0,
            ctx.get_settings().get_max_threads()? as usize,
            blocks.len(),
        );
        Ok((statistics, parts))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let numbers = ctx.get_settings().get_max_threads()? as usize;
        let read_data_blocks = self.get_read_data_blocks();

        // Add source pipe.
        pipeline.add_source(
            |output| {
                MemoryTableSource::create(
                    ctx.clone(),
                    output,
                    plan.schema(),
                    read_data_blocks.clone(),
                    plan.push_downs.clone(),
                )
            },
            numbers,
        )
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _: AppendMode,
        _: bool,
    ) -> Result<()> {
        pipeline.add_sink(|input| Ok(ContextSink::create(input, ctx.clone())))
    }

    async fn commit_insertion(
        &self,
        _: Arc<dyn TableContext>,
        operations: Vec<Chunk>,
        overwrite: bool,
    ) -> Result<()> {
        let written_bytes: usize = operations.iter().map(|b| b.memory_size()).sum();

        self.data_metrics.inc_write_bytes(written_bytes);

        if overwrite {
            let mut blocks = self.blocks.write();
            blocks.clear();
        }
        let mut blocks = self.blocks.write();
        for block in operations {
            blocks.push(block);
        }
        Ok(())
    }

    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _: bool) -> Result<()> {
        let mut blocks = self.blocks.write();
        blocks.clear();
        Ok(())
    }
}

struct MemoryTableSource {
    extras: Option<PushDownInfo>,
    schema: TableSchemaRef,
    data_blocks: Arc<Mutex<VecDeque<Chunk>>>,
}

impl MemoryTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        schema: TableSchemaRef,
        data_blocks: Arc<Mutex<VecDeque<Chunk>>>,
        extras: Option<PushDownInfo>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, MemoryTableSource {
            extras,
            schema,
            data_blocks,
        })
    }

    fn projection(&self, chunk: Chunk) -> Result<Option<Chunk>> {
        if let Some(extras) = &self.extras {
            if let Some(projection) = &extras.projection {
                let num_rows = chunk.num_rows();
                let pruned_data_block = match projection {
                    Projection::Columns(indices) => {
                        let pruned_schema = self.schema.project(indices);
                        let columns = indices
                            .iter()
                            .map(|idx| chunk.get_by_offset(*idx).clone())
                            .collect();
                        Chunk::new(columns, num_rows)
                    }
                    Projection::InnerColumns(path_indices) => {
                        let pruned_schema = self.schema.inner_project(path_indices);
                        let mut columns = Vec::with_capacity(path_indices.len());
                        let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                        for path in paths {
                            let raw_columns = chunk
                                .columns()
                                .map(|entry| (entry.value.clone(), entry.data_type.clone()))
                                .collect::<Vec<_>>();
                            let column = Self::traverse_paths(&raw_columns, path)?;
                            columns.push(column);
                        }
                        Chunk::new_from_sequence(columns, num_rows)
                    }
                };
                return Ok(Some(pruned_data_block));
            }
        }

        Ok(Some(chunk))
    }

    fn traverse_paths(
        columns: &[(Value<AnyType>, DataType)],
        path: &[usize],
    ) -> Result<(Value<AnyType>, DataType)> {
        if path.is_empty() {
            return Err(ErrorCode::BadArguments("path should not be empty"));
        }
        let (value, data_type) = &columns[path[0]];
        if path.len() == 1 {
            return Ok((value.clone(), data_type.clone()));
        }

        match data_type {
            DataType::Tuple(inner_tys) => {
                let col = value.clone().into_column().unwrap();
                let (inner_columns, _) = col.into_tuple().unwrap();
                let mut values = Vec::with_capacity(inner_tys.len());
                for (col, ty) in inner_columns.iter().zip(inner_tys.iter()) {
                    values.push((Value::Column(col.clone()), ty.clone()))
                }
                Self::traverse_paths(&values[..], &path[1..])
            }
            _ => Err(ErrorCode::BadArguments(format!(
                "Unable to get column by paths: {:?}",
                path
            ))),
        }
    }
}

impl SyncSource for MemoryTableSource {
    const NAME: &'static str = "MemoryTable";

    fn generate(&mut self) -> Result<Option<Chunk>> {
        let mut blocks_guard = self.data_blocks.lock();
        match blocks_guard.pop_front() {
            None => Ok(None),
            Some(data_block) => self.projection(data_block),
        }
    }
}