package in.dreamlab.iisc.pagerank;

/**
 * Created by prateeksha on 27/1/16.
 */
/**
 * Created by prateeksha on 27/1/16.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TwitterTopoCounterCombiner extends Reducer<Text,Text, Text,Text> {


    @Override

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        try{
            double sum=0.0;
            String adjacency_list="";
            String page_rank;
            String str;

            for(Text t:values){
                str=t.toString().trim();
                if(str.charAt(str.length()-1)=='$'){
                    page_rank=str.substring(0,str.indexOf('$'));
                    sum=sum+Double.parseDouble(page_rank);
                }
                else{
                    adjacency_list=t.toString().trim();
                }



            }
           context.write(key,new Text(adjacency_list));
            context.write(key,new Text(String.valueOf(sum)+"$"));

        }
        catch (Exception ex) {
            //LOG.error("Caught Exception", ex);
            //context.getCounter(REDUCERCOUNTER.EXCEPTIONS).increment(1);
        }
    }
}




