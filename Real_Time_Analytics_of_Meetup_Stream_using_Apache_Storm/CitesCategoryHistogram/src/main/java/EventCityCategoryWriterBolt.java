// This bolt cannot be parallelized! //

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class EventCityCategoryWriterBolt implements IRichBolt 
{	
	private String eventLocationCategoryFile ;
	FileWriter fw;
	BufferedWriter bw;
	private OutputCollector collector;

   public EventCityCategoryWriterBolt(String eventLocationCategoryFile)
   {
		super();
		this.eventLocationCategoryFile = eventLocationCategoryFile;
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	   // Initialize the class members appropriately
	   try
	   {
		   fw = new FileWriter(eventLocationCategoryFile);
		   bw = new BufferedWriter(fw);
	   }
	   catch(IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   }
	   this.collector = collector;
   }

   
   public void execute(Tuple tuple) {
	   // Get the tuple, populate the HashMap and BloomFilter appropriately
	   String event_id = tuple.getString(0);
	   String location = tuple.getString(1);
	   String category = tuple.getString(2);
	   
	   try
	   {
  			bw.write(event_id + "," + location + "," + category + "\n");
  			bw.flush();
	   }
	   catch(IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   }
	   collector.ack(tuple);
   }

   
   public void cleanup() {
	   try
	   {
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
	
   
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}
