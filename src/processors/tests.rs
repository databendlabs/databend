// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_dag() {
    use std::sync::Arc;

    use crate::processors::{connect, Graph, PipelineExecutor, Processors};
    use crate::transforms::{SimpleTransform, SinkTransform, SourceTransform};

    let mut graph = Graph::default();
    let source = SourceTransform::create(graph.new_node());
    let simple = SimpleTransform::create(graph.new_node());
    let sink = SinkTransform::create(graph.new_node());

    connect(source.output_port(), simple.input_port()).unwrap();
    connect(simple.output_port(), sink.input_port()).unwrap();

    let mut processors = Processors::default();
    processors.add(source).unwrap();
    processors.add(simple).unwrap();
    processors.add(sink).unwrap();
    println!("{:?}", processors);

    let executor = PipelineExecutor::create(Arc::new(processors));
    executor.execute(4).unwrap();
    println!("result: {:?}", executor.result().unwrap());
}
