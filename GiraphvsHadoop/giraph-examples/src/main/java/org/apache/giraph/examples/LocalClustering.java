package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.*;
import org.apache.giraph.graph.Vertex;

import java.util.HashSet;


@Algorithm(
        name = "Hash Count"
)
public class LocalClustering extends Vertex<LongWritable,Text,
        FloatWritable, Text> {
//input is list of hashtags without initial value
    @Override
    public void compute(Iterable<Text> messages) {
        if(getSuperstep()==0){

                Text vertexValue = getValue();
                String str1 = vertexValue.toString().trim();
                String hash_tags[]=str1.split("\\$");
                for(int j=0;j<hash_tags.length;j++){

                    if (hash_tags[j].trim().equalsIgnoreCase("popularphoto")) {

                        LongWritable id=getId();
                        for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
                        sendMessage(edge.getTargetVertexId(), new Text(id.toString()));
                            System.out.println("message superstep 0"+id.toString());

                    }
                    break;
                    }
                    else{
                        setValue(new Text("0"));
                    }

               }

            voteToHalt();
        }
        if(getSuperstep()==1){
            String value="";
           // HashSet<LongWritable> friends = new HashSet<LongWritable>();
            for (Text msg : messages) {

                 value=value+msg.toString()+",";

            }
            setValue(new Text(value.substring(0, value.length() - 1)));
            System.out.println("value at superstep 1 is" + value);
            sendMessageToAllEdges(new Text(getId().toString() + "," + value.substring(0, value.length() - 1)));
            System.out.println("msg sent at superstep 1 is"+getId().toString() + "," + value.substring(0, value.length() - 1));



            voteToHalt();



        }
        if(getSuperstep()==2){
            int count=0;
            int edges=0;

            HashSet friends = new HashSet();
            friends.add(getId().toString());

            String str=getValue().toString();
            String[] vertices=str.split(",");
            for(String s:vertices){
                friends.add(s);
                count++;
            }
            edges=count+1;
for(Text t:messages){
    String m=t.toString();
    String ids[]=m.split(",");
    for(int i=1;i<ids.length;i++){

            if(friends.contains(ids[i])&&friends.contains(ids[0])){
                count++;
            }


    }

}
            count=count/2;
            System.out.println("edges are"+edges+"count is"+count);
            double clustering_coefficient=(double)count/(double)((edges*(edges-1))/2);
            setValue(new Text(String.valueOf(clustering_coefficient)));
            voteToHalt();
        }




    }





}










