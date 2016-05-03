import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.HttpClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
	
public class GenerateAcceptableUsersFromStreamSpout implements IRichSpout {
   static String meetup_rsvp_api_url = "http://stream.meetup.com/2/rsvps"; 
   private HttpClient client;
   static JSONParser jsonParser = new JSONParser();

	//Create instance for SpoutOutputCollector which passes tuples to bolt
   private SpoutOutputCollector collector;
   //Create instance for TopologyContext which contains topology data
   private TopologyContext context;
	
   private Integer idx = 0;


   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      this.context = context;
      this.collector = collector;
   }

   
   public void nextTuple() {
       client = HttpClientBuilder.create().build();
       HttpGet request = new HttpGet(meetup_rsvp_api_url);
       HttpResponse response;
       try {
           response = client.execute(request);
           StatusLine status = response.getStatusLine();
           if(status.getStatusCode() == 200){
               InputStream inputStream = response.getEntity().getContent();
               BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
               String rsvp;

               // Iterate till there are entries for RSVPs available in the response,
               // or till we have hit 100 records
               while((rsvp = reader.readLine())!=null && idx < 100){
                   try{
                	   JSONObject jsonObject  = (JSONObject) jsonParser.parse(rsvp);
                	   
                	   // Parse and get the member sub-object from the JSON object
                	   JSONObject member = (JSONObject) jsonObject.get("member");
                	   // From the member object, get the Long member_id value
                	   Long member_id = (Long) member.get("member_id");
                	   
                	   // Emit the member_id value as the only field for the Bolt to use
                       collector.emit(new Values(member_id));
                       idx++;
                   }catch (Exception e) {
                       System.out.println("Error parsing message from meetup: " + e.getMessage());
                   }
               }
           }
       } catch (IOException e) {
    	   System.out.println("Error in communication with meetup api ["+request.getURI().toString()+"]");
           try {
               Thread.sleep(10000);
           } catch (InterruptedException e1) {
           }
       }
				
      }


   
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("member_id"));
   }

   
   public void close() {}

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
   