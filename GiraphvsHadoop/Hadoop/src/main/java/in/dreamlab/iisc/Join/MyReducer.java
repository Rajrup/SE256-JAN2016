package in.dreamlab.iisc.Join;

/**
 * Created by prateeksha on 28/9/15.
 */
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.io.Text;


public class MyReducer extends Reducer< Text, Text,Text,Text> {

    public static long localcount = 0L;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        for(Text t:values){
            context.write(key,t);
            break;
        }

    }

}






