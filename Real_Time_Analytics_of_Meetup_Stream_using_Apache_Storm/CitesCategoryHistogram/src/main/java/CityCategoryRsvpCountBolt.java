import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class CityCategoryRsvpCountBolt implements IRichBolt {
	private Map<String, CategoryRsvpTracker> locationCategoryCount;
	private Map<Long, Integer> rsvpMap;

	private String locationCategoryRsvpCountFile ;
	private OutputCollector collector;
	private int emitFrequencySeconds;
	
	public CityCategoryRsvpCountBolt(String locationCategoryRsvpCountFile, int seconds) {
		super();
		this.locationCategoryRsvpCountFile = locationCategoryRsvpCountFile;
		this.emitFrequencySeconds = seconds;
	}
   
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	   this.locationCategoryCount = new HashMap<String, CategoryRsvpTracker>();
	   this.rsvpMap = new HashMap<Long, Integer>();
	   this.collector = collector;
   }

   
   public void execute(Tuple tuple) 
   {
	  if(isTickTuple(tuple))
	  	writeOutput();
	  else
	  {
	  	 Long rsvp_id = tuple.getLong(0);
		   String location = tuple.getString(1);
		   String category = tuple.getString(2);
		   
		   if(!rsvpMap.containsKey(rsvp_id))
		   {
			
			   if(!locationCategoryCount.containsKey(location))
			   {
				   Map<String, Integer> categoryCount = new HashMap<String, Integer>();
				   categoryCount.put(category, 1);
				   CategoryRsvpTracker categoryRsvpTracker = new CategoryRsvpTracker(1, categoryCount);
				   locationCategoryCount.put(location, categoryRsvpTracker);
			   }
			   else
			   {
				   CategoryRsvpTracker categoryCounter = locationCategoryCount.get(location);
				   if(!categoryCounter.categoryCount.containsKey(category))
				   {
					   categoryCounter.categoryCount.put(category, 1);
				   }
				   else
				   {
					   Integer value = categoryCounter.categoryCount.get(category) + 1;
					   categoryCounter.categoryCount.put(category, value);
				   }
				   categoryCounter.totalRsvpCount += 1;
				   locationCategoryCount.put(location, categoryCounter);
			   }
			   rsvpMap.put(rsvp_id, 1);
		   }
		   else
		   {
			   int rsvpCount = rsvpMap.get(rsvp_id) + 1;
			   rsvpMap.put(rsvp_id, rsvpCount);
		   } 
	  }		
   }

   
   public void writeOutput() 
   {
	   try
	   {
		   FileWriter fw = new FileWriter(locationCategoryRsvpCountFile);
		   BufferedWriter bw = new BufferedWriter(fw);
		   
		   	for(Entry<String, CategoryRsvpTracker> entry : locationCategoryCount.entrySet())
		   	{
		   		String location = entry.getKey();
		   		CategoryRsvpTracker categoryRsvpTracker = entry.getValue();
		   		int totalLocationEventCount = categoryRsvpTracker.totalRsvpCount;
		   		for(Entry<String, Integer> entryInner : categoryRsvpTracker.categoryCount.entrySet())
		   		{
		   			//System.out.println(location + "," + entryInner.getKey() + "," + entryInner.getValue() + "," + totalLocationEventCount);
		   			bw.write(location + "," + entryInner.getKey() + "," + entryInner.getValue() + "," + totalLocationEventCount + "\n");
		   		}
		   	}
	   
		   bw.close();
	       fw.close();
	       
	   }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   } 
   }

   
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("event_id", "location", "category"));
   }
	
   
   public Map<String, Object> getComponentConfiguration() 
   {
  	 Map<String, Object> conf = new HashMap<String, Object>();
     conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencySeconds);
     return conf;
   }

	public void cleanup() {
		// TODO Auto-generated method stub
	}
	
	public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
}
	
}
