import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

//import storm configuration packages
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class RsvpEventPopularityPerCityPerCategory
{
	public static void main(String[] args) throws Exception
	{
      Config config = new Config();
      config.setDebug(false);
      
      String toponame = args.length > 0 ? args[0] : "trending-events-topo";
  		String isCluster = args.length > 1 ? args[1] : "L";
  		String properties = args.length > 2 ? args[2] : "histogram-properties.json";	    
  		
      TopologyBuilder builder = new TopologyBuilder();
  		
      JSONParser parser = new JSONParser();
  		JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
  		String inputFilePath = (String) json.get("rsvp-input-file-path");
  		String outputFilePath = (String) json.get("rsvp-output-file-path");
  		String intermediateFile = (String) json.get("intermediate-file-path");
      int emitSeconds = Integer.parseInt((String) json.get("emit-freq"));
  		
      builder.setSpout("rsvp-event-spout", new RsvpEventSpout(inputFilePath));

      builder.setBolt("rsvp-city-category-bolt", new FilterRsvpOnEventBolt(intermediateFile))
         .fieldsGrouping("rsvp-event-spout", new Fields("event_id"));
			
      builder.setBolt("city-category-rsvp-count-bolt", new CityCategoryRsvpCountBolt(outputFilePath, emitSeconds))
      							.fieldsGrouping("rsvp-city-category-bolt", new Fields("location"));
      if (isCluster.trim().equals("C") ) 
      {
      	// set the number of workers for running all spout and bolt tasks
        StormSubmitter.submitTopology(toponame, config, builder.createTopology());
      }
      else
      {	
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology(toponame, config, builder.createTopology());
	      Thread.sleep(300000);
	      cluster.shutdown();
      }
   }
}

