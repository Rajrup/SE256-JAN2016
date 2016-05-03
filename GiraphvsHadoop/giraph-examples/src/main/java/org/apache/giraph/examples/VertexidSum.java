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
public class VertexidSum extends Vertex<LongWritable,Text,
        FloatWritable, DoubleWritable> {

    @Override
    public void compute(Iterable<DoubleWritable> messages) {
        System.out.println("begin.......");
        LongWritable v_id=getId();
        String s=String.valueOf(v_id);

        Text vertexValue = getValue();
        String str1=vertexValue.toString();
        String str=str1.trim();
        int i=str.indexOf('$');
        String val1,val2;
        System.out.println("mid......");
        if(i==-1)
        {
           val1=s;
           val2="0.0";
        }
        else {
            val2 = str.substring(0, i);
            val1 = s;
        }
        double v1=Double.parseDouble(val1);
        double v2=Double.parseDouble(val2);
        double v3=v2+(v1/10.0);
        String finalval=String.valueOf(v3);
        vertexValue.set(new Text(finalval));
        setValue(new Text(finalval));

        voteToHalt();


    }
}
