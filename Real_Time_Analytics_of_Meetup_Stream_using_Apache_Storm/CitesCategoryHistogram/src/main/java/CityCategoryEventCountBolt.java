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

public class CityCategoryEventCountBolt implements IRichBolt {
	private Map<String, CategoryEventTracker> locationCategoryCount;

	private Map<String, Integer> eventMap;
	
	private String locationCategoryEventCountFile ;
	private int emitFrequencySeconds;
	
	private OutputCollector collector;

   
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	   this.locationCategoryCount = new HashMap<String, CategoryEventTracker>();
	   this.eventMap = new HashMap<String, Integer>();
	   this.collector = collector;
   }

   
   public CityCategoryEventCountBolt(String locationCategoryEventCountFile, int emitFrequencySeconds) {
		super();
		this.locationCategoryEventCountFile = locationCategoryEventCountFile;
		this.emitFrequencySeconds = emitFrequencySeconds;
	}


	public void execute(Tuple tuple) 
	{
	   if(isTickTuple(tuple))
	   {
	  	 writeOutput();
	   }
	   else 
	   {
			 String event_id = tuple.getString(0);
		   String location = tuple.getString(1);
		   String category = tuple.getString(2);
		   
		   if(!eventMap.containsKey(event_id))
		   {
			
			   if(!locationCategoryCount.containsKey(location))
			   {
				   Map<String, Integer> categoryCount = new HashMap<String, Integer>();
				   categoryCount.put(category, 1);
				   CategoryEventTracker categoryEventTracker = new CategoryEventTracker(1, categoryCount);
				   locationCategoryCount.put(location, categoryEventTracker);
			   }
			   else
			   {
				   CategoryEventTracker categoryCounter = locationCategoryCount.get(location);
				   if(!categoryCounter.categoryCount.containsKey(category))
				   {
					   categoryCounter.categoryCount.put(category, 1);
				   }
				   else
				   {
					   Integer value = categoryCounter.categoryCount.get(category) + 1;
					   categoryCounter.categoryCount.put(category, value);
				   }
				   categoryCounter.totalEventCount += 1;
				   locationCategoryCount.put(location, categoryCounter);
			   }
			   eventMap.put(event_id, 1);
			   collector.emit(new Values(event_id, location, category));
		   }
	   }
   }

   
   public void writeOutput() {
      
	   try
	   {
		   FileWriter fw = new FileWriter(locationCategoryEventCountFile);
		   BufferedWriter bw = new BufferedWriter(fw);
		   
		   	for(Entry<String, CategoryEventTracker> entry : locationCategoryCount.entrySet())
		   	{
		   		String location = entry.getKey();
		   		CategoryEventTracker categoryEventTracker = entry.getValue();
		   		int totalLocationEventCount = categoryEventTracker.totalEventCount;
		   		for(Entry<String, Integer> entryInner : categoryEventTracker.categoryCount.entrySet())
		   		{
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
	
 	public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
        && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
}


	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
}
