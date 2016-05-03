package in.dreamlab.iisc.Join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperState extends Mapper<LongWritable , Text, Text, Text> {
    private HashMap mappingHm=new HashMap();
    String j;



    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
			/*BufferedReader in = new BufferedReader(new FileReader("hdfs://sslcluster:9000/user/secprav/mapping/mapping.txt"));
			String str;

			List<String> list = new ArrayList<String>();
			while((str = in.readLine()) != null){
				list.add(str);
			}*/
        j = conf.get("i");
        if (!j.equals("0")) {
            String file_name = conf.get("file_name");


            FileSystem fs = FileSystem.get(new Configuration());
            List<String> list = new ArrayList<String>();
            FileStatus[] status = fs.listStatus(new Path("hdfs://10.0.0.1:54322" + file_name));
            for (int i = 0; i < status.length; i++) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line = br.readLine();
                while (line != null) {
                    //System.out.println(line);
                    list.add(line);
                    line = br.readLine();
                }
            }


            String[] mapping = list.toArray(new String[0]);
            String count;
            String id;
            String url_id[];


            for (int i = 0; i < mapping.length; i++) {
                //id=mapping[i].substring(0,mapping[i].indexOf('h')).trim();
                //url=mapping[i].substring(mapping[i].indexOf('h'),mapping[i].length()).trim();
                url_id = mapping[i].split("\\s+");
                id = url_id[0].trim();
                count = url_id[1].trim();


                mappingHm.put(id, count);
            }
        }
				/*String[] arr=mapping[i].split("\\t+");
				url=arr[1].trim();
				id=arr[0].trim();
				mappingHm.put(url, Integer.parseInt(id));*/
    }
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String data=value.toString();
        int index=data.indexOf(",");
        String id=data.substring(0, index);
        String id_to_print=data.substring(0, index + 1);
        String rest_data_to_print=data.substring(index+1,data.length());
        String processed_id=id.substring(1,id.length());
        if(!j.equals("0")){




            String count=(String)mappingHm.get(processed_id);
            context.write(new Text(id_to_print+""+count+"$"+rest_data_to_print),new Text(""));}
        else{
            context.write(new Text(id_to_print+"0"+"$"+rest_data_to_print),new Text(""));
        }






    }

}

