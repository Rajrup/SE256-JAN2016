import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class FilterRsvpOnEventBolt implements IRichBolt {
	
	// Files where the serialized copies of the acceptable members HashMap and BloomFilters are stored
	private String eventLocationCategoryFile ;
	private FileReader fileReader;
	private BufferedReader bufferedReader;
	private Map<String, EventDetails> eventDetailsMap;
	private int totalCount;
	private int selectedCount;
	private OutputCollector collector;
 
   public FilterRsvpOnEventBolt(String eventLocationCategoryFile) {
		super();
		this.eventLocationCategoryFile = eventLocationCategoryFile;
	}


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	   // Initialize the class members appropriately
	   totalCount = 0;
	   selectedCount = 0;
	   try
	   {
		   fileReader = new FileReader(eventLocationCategoryFile);
		   bufferedReader = new BufferedReader(fileReader);
		   
		   eventDetailsMap = new HashMap<String, EventDetails>();
		   
		   String event;
		   
           while((event = bufferedReader.readLine())!=null /* && idx < 100 */)
           {
        	   String [] eventArray = event.split(",");
        	   String event_id = eventArray[0];
        	   String location = eventArray[1];
        	   String category = eventArray[2];
        	   
        	   if(!eventDetailsMap.containsKey(event_id))
        	   {
        		   EventDetails eventDetails = new EventDetails(location, category);
        		   eventDetailsMap.put(event_id, eventDetails);
        	   }
           }
           
           bufferedReader.close();
           fileReader.close();
	   }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
       }
	   catch (Exception e)
	   {
    	   System.out.println("Other problems: " + e.getMessage());
	   }

	   this.collector = collector;
   }

   
   public void execute(Tuple tuple) {
	   // Get the tuple, populate the HashMap and BloomFilter appropriately
	   Long rsvp_id = tuple.getLong(0);
	   String event_id = tuple.getString(1);
	   totalCount++;
	   if(eventDetailsMap.containsKey(event_id))
	   {
		   selectedCount++;
		   EventDetails eventDetails = eventDetailsMap.get(event_id);
		   collector.emit(new Values(rsvp_id, eventDetails.location, eventDetails.category));
	   }
   }

   
   public void cleanup() {
	   System.out.println("Total Count: " + totalCount);
	   System.out.println("Selected Count: " + selectedCount);
   }

   
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("rsvp_id", "location", "category"));
   }
	
   
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
}
