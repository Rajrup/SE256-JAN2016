package in.dreamlab.iisc.trans;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
//import in.ac.iisc.cds.se256.alpha.CC.pojo.IntTextPairWritable;

public class CCAnalyticsWebCrawlerCombine
{
    private static final Logger LOG = Logger.getLogger(CCAnalyticsWebCrawlerCombine.class);

    protected static enum COMBINERCOUNTER
    {
        COMBINER_WRITE,
        EXCEPTIONS
    }

    protected static class CCAnalyticsWebCrawlerCombiner extends Reducer<Text,Text, Text,Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            try{
                double sum=0.0;
                 for(Text t:values){
                    sum=sum+Double.parseDouble(t.toString());
                 }
                context.write(key,new Text(String.valueOf(sum)));





            }
            catch(Exception Ex)
            {
                LOG.error("Caught Exception", Ex);
                //context.getCounter(REDUCERCOUNTER.EXCEPTIONS).increment(1);
            }
        }


    }
}