package org.apache.giraph.examples;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.*;
import org.apache.giraph.graph.Vertex;

import java.io.File;
import java.io.IOException;
import java.util.*;


@Algorithm(
        name = "Hash Count"
)
public class HashTagGeneration extends Vertex<LongWritable,Text,
        FloatWritable, Text> {

static String random_hashtags[];
    static HashMap hmap=new HashMap();
    @Override
    public void compute(Iterable<Text> messages) {
        int max=89,min=0;
        String hashtag;
        String myvalue="";
        if(getSuperstep()==0){
         for(int i=1;i<=5;i++){
             Random random = new Random();
             int index=random.nextInt(90);
             hashtag=random_hashtags[index]+"0";
             myvalue=myvalue+hashtag+"$";
         }
            setValue(new Text(myvalue.substring(0,myvalue.length()-1)));
            sendMessageToAllEdges(new Text(myvalue.substring(0,myvalue.length()-1)));
           // hmap.put();
        }





    }
    public static class AggregatorsTestMasterCompute extends
            DefaultMasterCompute {
        public void initialize() throws InstantiationException,
                IllegalAccessException {
            try{
                String token1="";
                Scanner inFile1 = new Scanner(new File("/home/prateeksha/random_hashtags")).useDelimiter("\\s+");

                // Original answer used LinkedList, but probably preferable to use ArrayList in most cases
                // List<String> temps = new LinkedList<String>();

                List<String> temps = new ArrayList<String>();

                // while loop
                while (inFile1.hasNext()) {
                    // find next line
                    token1 = inFile1.nextLine();
                    temps.add(token1);
                }
                inFile1.close();

                random_hashtags = temps.toArray(new String[0]);}
            catch(Exception e){

            }


        }

        public void compute(){
            System.out.println("Hashtag count till now is : "+getAggregatedValue("persistent"));

        }


    }


}










