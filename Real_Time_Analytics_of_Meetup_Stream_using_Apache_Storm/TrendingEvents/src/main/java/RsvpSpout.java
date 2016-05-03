import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class RsvpSpout extends BaseRichSpout {

	private static final long serialVersionUID = 2996375525800042840L;
	private String filePath;	
	private SpoutOutputCollector collector;	
	private final int sleep;
	private int counter;
	private BufferedReader br;
	String line = null;
	
	
	public RsvpSpout(String filePath, int sleep) 
	{
		this.filePath = filePath;
		this.sleep = sleep;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) 
	{

		collector = soc;
		counter = 0;
		try{
		  br = new BufferedReader(new FileReader(filePath));
		}
		catch(Exception e){
			System.out.println("Exception occurred in RsvpSpout open()");
			e.printStackTrace();
		}
		
	}
	
	public void nextTuple() {	
		
		Utils.sleep(sleep);		
		try
		{
			//if (counter < rsvpList.size()) {		
			if((line = br.readLine()) != null) {
				JSONParser parser = new JSONParser();
				JSONObject json =  (JSONObject) parser.parse(line);
				String response = (String) json.get("response");
					
				JSONObject event = (JSONObject) json.get("event");
				String event_id = (String) event.get("event_name");
				//System.out.println(event_id + " " + response.toLowerCase());
				collector.emit(new Values(event_id, response.toLowerCase()));
				counter++;
			}		
		}
		catch(Exception e){
			System.out.println("Exception occurred in RsvpSpout nextTuple()");
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		System.out.println("Records Processed: " + counter);
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.close();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event_id", "response"));
	}

}
