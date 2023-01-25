            return Ok(Event::NeedData);
        }

        if !self.buckets_blocks.is_empty() && !self.unsplitted_blocks.is_empty() {
            // Split data blocks if it's unsplitted.
            return Ok(Event::Sync);
        }

        if !self.output.can_push() {
            for input_state in &self.inputs {
                input_state.port.set_not_need_data();
            }

            return Ok(Event::NeedConsume);
        }

        let pushed_data_block = self.try_push_data_block();

        loop {
            // Try to pull the next data or until the port is closed
            let mut all_inputs_is_finished = true;
            let mut all_port_prepared_data = true;

            for index in 0..self.inputs.len() {
                if self.inputs[index].port.is_finished() {
                    continue;
                }

                all_inputs_is_finished = false;
                if self.inputs[index].bucket >= self.working_bucket {
                    continue;
                }

                self.inputs[index].port.set_need_data();
                if !self.inputs[index].port.has_data() {
                    all_port_prepared_data = false;
                    continue;
                }

                self.inputs[index].bucket =
                    self.add_bucket(self.inputs[index].port.pull_data().unwrap()?);
                debug_assert!(self.unsplitted_blocks.is_empty());
            }

            if all_inputs_is_finished {
                self.all_inputs_is_finished = true;
                break;
            }

            if !all_port_prepared_data {
                return Ok(Event::NeedData);
            }

            self.working_bucket += 1;
        }

        if pushed_data_block || self.try_push_data_block() {
            return Ok(Event::NeedConsume);
        }

        self.output.finish();
        Ok(Event::Finished)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.unsplitted_blocks.pop() {
            let data_block_meta: Option<&AggregateInfo> = data_block
                .get_meta()
                .and_then(|meta| meta.as_any().downcast_ref::<AggregateInfo>());

            let data_blocks = match data_block_meta {
                None => self.convert_to_two_level(data_block)?,
                Some(meta) => match &meta.overflow {
                    None => self.convert_to_two_level(data_block)?,
                    Some(_overflow_info) => unimplemented!(),
                },
            };

            for (bucket, block) in data_blocks.into_iter().enumerate() {
                if !block.is_empty() {
                    match self.buckets_blocks.entry(bucket as isize) {
                        Entry::Vacant(v) => {
                            v.insert(vec![block]);
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().push(block);
                        }
                    };
                }
            }
        }

        Ok(())
    }
}

fn build_convert_grouping<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static>(
    method: Method,
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    let input_nums = pipeline.output_len();
    let transform = TransformConvertGrouping::create(method.clone(), params.clone(), input_nums)?;

    let output = transform.get_output();
    let inputs_port = transform.get_inputs();

    pipeline.add_pipe(Pipe::ResizePipe {
        inputs_port,
        outputs_port: vec![output],
        processor: ProcessorPtr::create(Box::new(transform)),
    });

    pipeline.resize(input_nums)?;

    pipeline.add_transform(|input, output| {
        MergeBucketTransform::try_create(input, output, method.clone(), params.clone())
    })
}

pub fn efficiently_memory_final_aggregator(
    params: Arc<AggregatorParams>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let group_cols = &params.group_columns;
    let schema_before_group_by = params.input_schema.clone();
    let sample_block = DataBlock::empty_with_schema(schema_before_group_by);
    let method = DataBlock::choose_hash_method(&sample_block, group_cols)?;

    with_hash_method!(|T| match method {
        HashMethodKind::T(v) => build_convert_grouping(v, pipeline, params.clone()),
    })
}

struct MergeBucketTransform<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static> {
    method: Method,
    params: Arc<AggregatorParams>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_block: Option<DataBlock>,
    output_blocks: Vec<DataBlock>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static>
    MergeBucketTransform<Method>
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MergeBucketTransform {
            input,
            output,
            method,
            params,
            input_block: None,
            output_blocks: vec![],
        })))
    }
}

#[async_trait::async_trait]
impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static> Processor
    for MergeBucketTransform<Method>
{
    fn name(&self) -> String {
        String::from("MergeBucketTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input_block.take();
            self.output_blocks.clear();
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_blocks.pop() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_block.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_block = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_block.take() {
            let mut blocks = vec![];
            if let Some(meta) = data_block.get_meta() {
                if let Some(meta) = meta.as_any().downcast_ref::<ConvertGroupingMetaInfo>() {
                    blocks.extend(meta.blocks.iter().cloned());
                }
            }

            match self.params.aggregate_functions.is_empty() {
                true => {
                    let mut bucket_merger = BucketAggregator::<false, _>::create(
                        self.method.clone(),
                        self.params.clone(),
                    )?;

                    self.output_blocks
                        .extend(bucket_merger.merge_blocks(blocks)?);
                }
                false => {
                    let mut bucket_merger = BucketAggregator::<true, _>::create(
                        self.method.clone(),
                        self.params.clone(),
                    )?;

                    self.output_blocks
                        .extend(bucket_merger.merge_blocks(blocks)?);
                }
            };
        }

        Ok(())
    }
}
