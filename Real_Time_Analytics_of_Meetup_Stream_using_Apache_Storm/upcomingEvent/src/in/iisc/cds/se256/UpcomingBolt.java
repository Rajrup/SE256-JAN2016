package in.iisc.cds.se256;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.lang.Integer;
import java.text.SimpleDateFormat;

public class UpcomingBolt implements IRichBolt{
	private OutputCollector collector;
	private String date;

	private BufferedWriter file_output1;
	private FileSystem fs1;
	private BufferedWriter file_output2;
	private FileSystem fs2;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;
		date = conf.get("date").toString();
		
		Configuration configuration = new Configuration();

		configuration.addResource(new Path(conf.get("core-site").toString()));
		configuration.addResource(new Path(conf.get("hdfs-site").toString()));

		try{
			fs1 = FileSystem.newInstance(configuration);
			Path pt = new Path("/SSDSProject/data/upcomingEvent/UpcomingBolt");

			if (fs1.exists(pt)) { // delete output folder if already present
				fs1.delete(pt, true);
			}

			file_output1 = new BufferedWriter(new OutputStreamWriter(fs1.create(pt)));

			fs2 = FileSystem.newInstance(configuration);
			pt = new Path("/SSDSProject/data/upcomingEvent/UpcomingDisplay");

			if (fs2.exists(pt)) { // delete output folder if already present
				fs2.delete(pt, true);
			}

			file_output2 = new BufferedWriter(new OutputStreamWriter(fs2.create(pt)));
		} catch(Exception e){
			throw new RuntimeException("Error Emitting tuple", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		try{
			String event_time = new SimpleDateFormat("yyyy,MM,dd,HH,mm,ss").format(
				new Date(input.getLongByField("event_time").longValue()));
			String [] eventTimeDetail = event_time.split(",");
			String event_date = eventTimeDetail[0];
			for(int i = 1; i < 3; i++){
				event_date += ("," + eventTimeDetail[i]);
			}

			if(event_date.equals(date)){
				String start_time = input.getStringByField("time");
				String end_time = new SimpleDateFormat("yyyy,MM,dd,HH,mm,ss,SSS").format(new Date());
				String msg_id = input.getStringByField("tuple_id");
				String event_name = input.getStringByField("event_name");
				String country = input.getStringByField("country");
				String city = input.getStringByField("city");
				String status = input.getStringByField("status");

				file_output1.append("UpcomingBolt" + "," + msg_id + "," + start_time + "," + end_time);
				file_output1.newLine();
				file_output2.append("UpcomingDisplay" + "\tStatus\t" + status + "\tCountry\t" + country + 
					"\tCity\t" + city + "\tEvent Name\t" + event_name + "\tEvent Time\t" + event_time);
				file_output2.newLine();

				System.out.println("UpcomingBolt" + "," + msg_id + "," + start_time + "," + end_time);
				System.out.println("UpcomingDisplay" + "\tStatus\t" + status + "\tCountry\t" + country + 
					"\tCity\t" + city + "\tEvent Name\t" + event_name + "\tEvent Time\t" + event_time);
			}
		} catch (Exception e){
			throw new RuntimeException("Error processing tuple for count by Aggregator Bolt", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		try {
			file_output1.close();
			fs1.close();
			file_output2.close();
			fs2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}