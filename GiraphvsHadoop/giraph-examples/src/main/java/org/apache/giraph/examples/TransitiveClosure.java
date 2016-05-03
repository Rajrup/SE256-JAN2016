package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.*;
import org.apache.giraph.graph.Vertex;


@Algorithm(
        name = "Hash Count"
)
public class TransitiveClosure extends Vertex<LongWritable,Text,
        FloatWritable, DoubleWritable> {

    @Override
    public void compute(Iterable<DoubleWritable> messages) {
        long superstep = getSuperstep();
        LongWritable vertexid=getId();
        LongWritable id=new LongWritable();
        id.set(6009554);
        if(superstep<=4){
        if(superstep==0){
           if(vertexid.equals(id)){
               setValue(new Text("reachable"));
                sendMessageToAllEdges(new DoubleWritable(1));
               voteToHalt();

            }
        }
        else{
            setValue(new Text("reachable"));
            sendMessageToAllEdges(new DoubleWritable(1));
            voteToHalt();
        }}
        voteToHalt();




    }





}









