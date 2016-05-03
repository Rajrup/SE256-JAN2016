import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
	
public class EventCityCategorySpout implements IRichSpout
{
   static JSONParser jsonParser = new JSONParser();
   
   private String filePath ;
   
   private FileReader fileReader;
   private BufferedReader bufferedReader;
   private SpoutOutputCollector collector;
   private TopologyContext context;
   private Integer idx = 0;
   private String city ; 
   
   public EventCityCategorySpout(String filePath, String city ) 
   {
		super();
		this.filePath = filePath;
		this.city  = city;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
   {
      this.context = context;
      
      this.collector = collector;
	   try
	   {
		   fileReader = new FileReader(filePath);
		   bufferedReader = new BufferedReader(fileReader);
	   }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
       }
   }

   public void nextTuple() {
	   try
	   {
            String event;

               if((event = bufferedReader.readLine())!=null )
               {
                   try
                   {
                	   JSONObject jsonObject  = (JSONObject) jsonParser.parse(event);
                	   String event_id = (String) jsonObject.get("id");
                	   JSONObject groupObject = (JSONObject) jsonObject.get("group");
                	   String location = (String) groupObject.get("city");
                	   JSONObject categoryObject = (JSONObject) groupObject.get("category");
                	   String category = (String) categoryObject.get("name");
                	   
                       collector.emit(new Values(event_id, location, category));
                       idx++;
                   }
                   catch (Exception e) 
                   {
                       System.out.println("Error parsing message from meetup");
                       e.printStackTrace();
                   }
               }
       }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
       }
      }


   
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("event_id", "location", "category"));
   }

   
   public void close()
   {
       try
       {
    	   bufferedReader.close();
       }
       catch (IOException e)
       {
    	   System.out.println("Could not open file: " + e.getMessage());
       }
   }

   public boolean isDistributed() {
      return false;
   }

   
   public void activate() {}

    
   public void deactivate() {}

   
   public void ack(Object msgId) {}

   
   public void fail(Object msgId) {}

   
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
}
