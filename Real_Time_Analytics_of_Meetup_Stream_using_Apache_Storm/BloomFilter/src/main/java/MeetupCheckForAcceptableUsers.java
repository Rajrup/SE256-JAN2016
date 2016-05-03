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

public class MeetupCheckForAcceptableUsers
{
	public static void main(String[] args) throws Exception{
	  String topology = (args.length > 0) ? args[0] : "BloomFilterTest";
	  String mode = (args.length > 1) ? args[1] : "L";
	  String properties = (args.length > 2) ? args[2] : "properties.json";
	  JSONParser parser = new JSONParser();
	  JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
		  
		  
      Config config = new Config();
      config.setDebug(false);
      	
      TopologyBuilder builder = new TopologyBuilder();
      // Spout creates a stream of member_ids from RSVPs for the test streams
      String inputFile = json.get("bloom-filter-test-input").toString();
      builder.setSpout("meetup-rsvp-acceptable-members-spout", new GenerateTestUsersFromFileSpout(inputFile));

      // Bolt puts each unique member_id from the test stream into a HashMap and counts their occurrence
      // In addition, each member_id is checked for its existence in the persisted HashMap
      // and in the persisted BloomFilter (both deserialized to get this informations) 
      String bloomFilterSerialized = (String) json.get("bloom-filter-train-output");
	    String counterMapSerialized = (String) json.get("bloom-filter-train-hashmap-output");
	    String outputFilePathResults = (String) json.get("bloom-filter-test-output");
      builder.setBolt("meetup-rsvp-acceptable-members-bolt", 
    		  new CheckAgainstBloomAndHashBolt(bloomFilterSerialized, counterMapSerialized))
         .fieldsGrouping("meetup-rsvp-acceptable-members-spout", new Fields("member_id"));

      builder.setBolt("meetup-rsvp-acceptable-members-writer-bolt", new AcceptableUserReportBolt(outputFilePathResults), 1)
		.globalGrouping("meetup-rsvp-acceptable-members-bolt");
			
      if (mode.equals("C")) 
      {
      	StormSubmitter.submitTopology(topology, config, builder.createTopology());
      }
      else
      {
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology(topology, config, builder.createTopology());
	      Thread.sleep(30000);			
	      cluster.shutdown();
      }
   }
}


