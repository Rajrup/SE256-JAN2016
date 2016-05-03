package org.apache.giraph.examples;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.graph.Vertex;



@Algorithm(
    name = "Simple State"
)
public class SimpleState extends Vertex<LongWritable, DoubleWritable,
        FloatWritable, DoubleWritable> {

  @Override
  public void compute(Iterable<DoubleWritable> messages) {
    DoubleWritable vertexValue = getValue();
    int val=(int)vertexValue.get();
    vertexValue.set(val/2);
    setValue(vertexValue);
    voteToHalt();
  }
}

