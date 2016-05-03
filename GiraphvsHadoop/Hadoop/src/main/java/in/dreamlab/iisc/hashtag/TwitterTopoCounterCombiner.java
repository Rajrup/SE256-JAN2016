package in.dreamlab.iisc.hashtag;

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

public class TwitterTopoCounterCombiner extends Reducer<Text,Text, Text, Text> {


    //private LongWritable in_degree_count = new LongWritable(0);


		/*protected void setup(Context context) throws IOException, InterruptedException {
			vertexCount = 0;
		}*/


    public void reduce(Text key, Iterable<Text> values, Context context) {
        try{


            long sum=0;
            for(Text t:values){
                sum=sum+Long.parseLong(t.toString());
            }
            context.write(new Text(key),new Text(String.valueOf(sum)));

        }
        catch (Exception ex) {
                    }
    }
}




