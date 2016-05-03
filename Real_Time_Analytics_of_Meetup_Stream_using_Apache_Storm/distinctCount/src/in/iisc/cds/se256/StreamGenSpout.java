package in.iisc.cds.se256;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.lang.Integer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

public class StreamGenSpout implements IRichSpout {

	private SpoutOutputCollector collector;
	private boolean completed = false;
	private TopologyContext context;

	private int numSpouts;
	private int filesPerSpout;
	private int spoutIdx;
	private int numFiles;
	private int filesForCurrSpout;

	private ArrayList<JSONObject> msgs = new ArrayList<JSONObject>();
	private int msgs_len = 0;
	private int msg_counter = 0;

	private int sleep_dur;

	private FileSystem fs;
	private BufferedWriter file_output;

	private String fileToRead(int fileReadCount){
		return "fileNo_"+( spoutIdx*filesPerSpout + fileReadCount);
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			filesPerSpout = Integer.parseInt(conf.get("filesPerSpout").toString());
			numSpouts = Integer.parseInt(conf.get("numSpouts").toString());
			numFiles = Integer.parseInt(conf.get("numFiles").toString());
			sleep_dur = Integer.parseInt(conf.get("sleep_dur").toString());
			spoutIdx = context.getThisTaskIndex();
			filesForCurrSpout = (spoutIdx < (numSpouts-1)) ? filesPerSpout : (numFiles-filesPerSpout*(numSpouts-1));

			this.context = context;
			Configuration configuration = new Configuration();

			configuration.addResource(new Path(conf.get("core-site").toString()));
			configuration.addResource(new Path(conf.get("hdfs-site").toString()));
			Path pt;
			String str;

			for(int i = 0; i < filesForCurrSpout; i++){
				// System.out.println("**************Spout Index "+spoutIdx+" Key for File "+fileToRead(i));
				// System.out.println("**************Spout Index "+spoutIdx+" Path for File "+conf.get(fileToRead(i)).toString());
				fs = FileSystem.newInstance(configuration);
				pt = new Path(conf.get(fileToRead(i)).toString());

				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObject;

				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));;
				while ((str = reader.readLine()) != null) {
					try{
						jsonObject  = (JSONObject) jsonParser.parse(str);
						msgs.add(jsonObject);
					} catch(Exception e){
						continue;
					}
				}
				reader.close();
				fs.close();
			}
			msgs_len = msgs.size();

			fs = FileSystem.newInstance(configuration);
			pt = new Path("/SSDSProject/data/uniqueCount/Spout_"+spoutIdx);

			if (fs.exists(pt)) { // delete output folder if already present
				fs.delete(pt, true);
			}

			file_output = new BufferedWriter(new OutputStreamWriter(fs.create(pt)));

		} catch (Exception e) {
			throw new RuntimeException("Error creating ArrayList of msgs", e);
		}
		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		if (completed) {
			try {
				file_output.close();
				fs.close();
				Thread.sleep(sleep_dur);
			} catch (Exception e) {
				throw new RuntimeException("Error Closing File", e);
			}
		}

		if(!completed){
			try {
				if(msg_counter < msgs_len){
					JSONObject event = (JSONObject) msgs.get(msg_counter).get("event");
					JSONObject group = (JSONObject) msgs.get(msg_counter).get("group");
					JSONObject member = (JSONObject) msgs.get(msg_counter).get("member");

					String event_name = (String ) event.get("event_name");
					String event_id = (String ) event.get("event_id");
					Long event_time = (Long ) event.get("time");

					String group_city = (String ) group.get("group_city");
					String group_country = (String ) group.get("group_country");
					Long group_id = (Long ) group.get("group_id");

					Long member_id = (Long ) member.get("member_id");

					Long msg_id = (Long ) msgs.get(msg_counter).get("rsvp_id");
					String time = new SimpleDateFormat("yyyy,MM,dd,HH,mm,ss,SSS").format(new Date());

					collector.emit(new Values(msg_id, time, group_id, group_country, 
						group_city, event_name, event_id, event_time, member_id));
					msg_counter++;

					file_output.append("Spout_" + spoutIdx + "," + msg_id + "," + time);
					file_output.newLine();
					System.out.println("Spout_" + spoutIdx + "," + msg_id + "," + time);
					System.out.println("Spout_" + spoutIdx + " emitted " + msg_counter + " tuples.");
				}
				else{
					completed = true;
				}
			} catch (Exception e) {
				throw new RuntimeException("Error Emitting tuple", e);
			}
		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple_id", "time", "group_id", "group_country", 
			"group_city", "event_name", "event_id", "event_time", "member_id"));
	}

	@Override
	public void close() {
		try {
			file_output.close();
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public boolean isDistributed() {
		return false;
	}
	@Override
	public void activate() {
	}
	@Override
	public void deactivate() {
	}
	@Override
	public void ack(Object msgId) {
	}
	@Override
	public void fail(Object msgId) {
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}