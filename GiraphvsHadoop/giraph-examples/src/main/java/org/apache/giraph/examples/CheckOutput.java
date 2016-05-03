package org.apache.giraph.examples;

import org.apache.giraph.examples.Algorithm;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Text;


@Algorithm(
        name = "Simple State"
)
public class CheckOutput extends Vertex<LongWritable,Text,
        FloatWritable, DoubleWritable> {

    @Override
    public void compute(Iterable<DoubleWritable> messages) {

        LongWritable v_id=getId();
        String message;

        String s=v_id.toString().trim();
        int vid=Integer.parseInt(s);
        long vval;
        int val;
        Text vertexValue = getValue();
        String str1=vertexValue.toString();
        String str=str1.trim();
        int i=str.indexOf('$');
        String val1,val2;
        if(i==-1)
        {

            val2=str;
        }
        else {
            val2 = str.substring(0, i);

        }
        double v2=Double.parseDouble(val2);
        vval=java.lang.Math.round(v2);
        val=(int)vval;
        if(vid==val)
        {   message="correct";
            vertexValue.set(new Text(message));
            setValue(new Text(message));


        }
        else
        {
            message="incorrect";
            vertexValue.set(new Text(message));
            setValue(new Text(message));


        }
        voteToHalt();


    }
}
