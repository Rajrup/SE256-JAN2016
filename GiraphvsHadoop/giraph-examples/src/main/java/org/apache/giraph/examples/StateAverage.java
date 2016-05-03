package org.apache.giraph.examples;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;


@Algorithm(
        name = "Simple State"
)
public class StateAverage extends Vertex<LongWritable,Text,
        FloatWritable, DoubleWritable> {

    @Override
    public void compute(Iterable<DoubleWritable> messages) {
        Text vertexValue = getValue();
        String str1=vertexValue.toString();
        String str=str1.trim();
        int i=str.indexOf('$');
        String val1,val2;
         if(i==-1)
         {
           val1="0";
           val2=str;
         }
        else {
             val1 = str.substring(0, i);
             val2 = str.substring(i + 1, str.length());
         }
        double v1=Double.parseDouble(val1);
        double v2=Double.parseDouble(val2);
        double v3=v1+v2;
        String finalval=String.valueOf(v3);
        vertexValue.set(new Text(finalval));
        setValue(new Text(finalval));
        voteToHalt();






    }
}
