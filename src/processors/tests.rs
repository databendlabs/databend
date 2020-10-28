// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_dag() {
    use crate::processors::{connect, Graph, Processors};
    use crate::transforms::{SimpleTransform, SinkTransform, SourceTransform};

    let mut graph = Graph::default();
    let mut source = SourceTransform::create(graph.new_node());
    let mut simple = SimpleTransform::create(graph.new_node());
    let mut sink = SinkTransform::create(graph.new_node());

    connect(source.get_output_ports()[0], simple.get_input_ports()[0]).unwrap();
    connect(simple.get_output_ports()[0], sink.get_input_ports()[0]).unwrap();

    let mut processors = Processors::default();
    processors.add(source);
    processors.add(simple);
    processors.add(sink);
    println!("{:?}", processors);
}
