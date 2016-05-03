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


public class MeetupAcceptableUsersFilterPreparation
{
	public static void main(String[] args) throws Exception
	{
		
    String topology = (args.length > 0) ? args[0] : "BloomFilterPrep";
	  String mode = (args.length > 1) ? args[1] : "L";
	  String properties = (args.length > 2) ? args[2] : "properties.json";
	  
	  JSONParser parser = new JSONParser();
	  JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
	  
		  
      Config config = new Config();
      config.setDebug(false);
      	
      TopologyBuilder builder = new TopologyBuilder();
      
      // Spout creates a stream of member_ids from RSVPs stream (stored in local file)
      builder.setSpout("meetup-rsvp-acceptable-members-spout", 
    		  new GenerateAcceptableUsersFromFileSpout((String) json.get("bloom-filter-train-input")));

      // Bolt puts each unique member_id into a HashMap and counts their occurrence
      // In addition, each member_id is put into a BloomFilter of size 100 with 
      // false positive probability (fpp) of max 0.1
      // We can update the size and fpp in the Bolt file while creating the BloomFilter object
      // to suit the size of data
      String bloomFilterSerialized = (String) json.get("bloom-filter-train-output");
	    String counterMapSerialized = (String) json.get("bloom-filter-train-hashmap-output");
      builder.setBolt("meetup-rsvp-acceptable-members-bolt", new BuildAcceptableUsersBloomBolt(bloomFilterSerialized, counterMapSerialized))
         .fieldsGrouping("meetup-rsvp-acceptable-members-spout", new Fields("member_id"));
			
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


