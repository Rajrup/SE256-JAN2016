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
public class HashCount extends Vertex<LongWritable,Text,
        FloatWritable, DoubleWritable> {

    @Override
    public void compute(Iterable<DoubleWritable> messages) {
        long superstep = getSuperstep();


        Text vertexValue = getValue();
        String str1 = vertexValue.toString().trim();

        String hash_count="";
        int i = str1.indexOf('$');
        int new_hash_count;


        hash_count = str1.substring(0, i);
        new_hash_count=Integer.parseInt(hash_count);
        String hash_list= str1.substring(i + 1, str1.length());

        String hash_tags[]=hash_list.split("\\$");
        for(int j=0;j<hash_tags.length;j++){

        if (hash_tags[j].trim().equalsIgnoreCase("popularphoto")) {

                new_hash_count=new_hash_count+1;

            }}

        hash_count=String.valueOf(new_hash_count);
        IntWritable val=new IntWritable(Integer.parseInt(hash_count));

        aggregate("persistent", val);





        vertexValue.set(new Text(hash_count));
        setValue(new Text(hash_count));
        voteToHalt();
    }
    public static class AggregatorsTestMasterCompute extends
            DefaultMasterCompute {
        public void initialize() throws InstantiationException,
                IllegalAccessException {
            registerPersistentAggregator("persistent",
                    IntSumAggregator.class);
        }

     public void compute(){
         System.out.println("Hashtag count till now is : "+getAggregatedValue("persistent"));

     }


    }


}









