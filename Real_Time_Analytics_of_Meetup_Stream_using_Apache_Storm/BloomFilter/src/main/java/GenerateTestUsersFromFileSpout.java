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
	
public class GenerateTestUsersFromFileSpout implements IRichSpout
{
   static JSONParser jsonParser = new JSONParser();

   // File path where the list of test members via RSVP stream JSON responses are kept
   private String filePath = "";

	// To read from the RSVPs text file
   private FileReader fileReader;
   private BufferedReader bufferedReader;

   private SpoutOutputCollector collector;
   private TopologyContext context;
	
   // Using this if we want to restrict the number of RSVP entries while testing the topology
   private Integer idx = 0;
   
   public GenerateTestUsersFromFileSpout(String filePath) 
   {
		this.filePath = filePath;
   }
   

   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
   {
      this.context = context;
      this.collector = collector;

      // Keep the RSVPs file open for nextTuple() phase
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
            String rsvp;

            // Iterate till there are entries for RSVPs available in the file
               if((rsvp = bufferedReader.readLine())!=null /* && idx < 100 */)
               {
                   try
                   {
                	   JSONObject jsonObject  = (JSONObject) jsonParser.parse(rsvp);
                	   
                	   // Parse and get the member sub-object from the JSON object
                	   JSONObject member = (JSONObject) jsonObject.get("member");
                	   // From the member object, get the Long member_id value
                	   Long member_id = (Long) member.get("member_id");
                	   
                	   // Emit the member_id value as the only field for the Bolt to use
                       collector.emit(new Values(member_id));
                       idx++;
                   }
                   catch (Exception e) 
                   {
                       System.out.println("Error parsing message from meetup: " + e.getMessage());
                   }
               }
       }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
       }
      }


   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("member_id"));
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
