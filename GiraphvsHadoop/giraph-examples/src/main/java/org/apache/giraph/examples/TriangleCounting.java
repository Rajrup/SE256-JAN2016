package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.*;
import org.apache.giraph.graph.Vertex;


@Algorithm(
        name = "Hash Count"
)
public class TriangleCounting extends Vertex<LongWritable,Text,
        FloatWritable, LongWritable> {

    @Override
    public void compute(Iterable<LongWritable> messages) {
       if(getSuperstep()==0){
           for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
               if (edge.getTargetVertexId().compareTo(getId()) > 0){
               LongWritable id=getId();
               sendMessage(edge.getTargetVertexId(), id);}
           }
           voteToHalt();
       }
        if(getSuperstep()==1){
            for (LongWritable msg : messages) {
                //assert(msg.compareTo(getId())<0);
                for (Edge<LongWritable,FloatWritable> edge: getEdges()) {
                    if (getId().compareTo(edge.getTargetVertexId()) < 0) {
                        sendMessage(edge.getTargetVertexId(), msg);
                    }
                }
            }
            voteToHalt();



        }
        if(getSuperstep()==2){


            int count = 0;
            for (LongWritable msg : messages) {

                if (getEdgeValue(msg)!=null) {
                    count++;
                }
            }
            if (count>0) {
                setValue(new Text(String .valueOf(count)));
            }
            voteToHalt();
        }




    }





}









