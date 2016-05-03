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

public class EventPopularityPerCityPerCategory
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
  		
  		String inputFilePath = (String) json.get("input-file-path");
  		String outputFilePath = (String) json.get("output-file-path");
  		String intermediateFilePath = (String) json.get("intermediate-file-path");
  		int emitfreq = Integer.parseInt((String)json.get("emit-freq"));
      String city = (String) json.get("city");
  		
  		builder.setSpout("event-city-category-spout", new EventCityCategorySpout(inputFilePath, city), 1 );

      builder.setBolt("city-category-event-count-bolt", new CityCategoryEventCountBolt(outputFilePath, emitfreq))
         .fieldsGrouping("event-city-category-spout", new Fields("location"));
			
      builder.setBolt("event-city-category-writer-bolt", new EventCityCategoryWriterBolt(intermediateFilePath))
      .fieldsGrouping("city-category-event-count-bolt", new Fields("location"));
      
      if (isCluster.trim().equals("C") ) 
      {
      	// set the number of workers for running all spout and bolt tasks
        StormSubmitter.submitTopology(toponame, config, builder.createTopology());
      }
      else
      {
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("MeetupStreamFilterProcessor", config, builder.createTopology());
	      Thread.sleep(300000);
	      cluster.shutdown();
      }
   }
}

