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

public class CountYesRsvpsDGIM
{
	public static void main(String[] args) throws Exception{
      Config config = new Config();
      config.setDebug(false);
      
      String toponame = args.length > 0 ? args[0] : "dgim-topo";
  		String isCluster = args.length > 1 ? args[1] : "L";
  		String properties = args.length > 2 ? args[2] : "dgim-properties.json";	
      
  		JSONParser parser = new JSONParser();
  		JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
  		
  		String inputfile = (String) json.get("input-file");
  		String outputfile = (String) json.get("output-file");
  		
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("rsvp-spout", new RsvpSpout(inputfile));

      builder.setBolt("dgim-count-yes-rsvp-bolt", new DGIMCountYesRsvpBolt(outputfile))
         .fieldsGrouping("rsvp-spout", new Fields("rsvpResponse"));
			
      if(isCluster.trim().equals("L"))
      {
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("MeetupStreamFilterProcessor", config, builder.createTopology());
	      Thread.sleep(100000);
	      cluster.shutdown();
      }
      else
      {
      	config.setNumWorkers(3);      
        config.registerSerialization( Bucket.class);
        StormSubmitter.submitTopology(toponame, config, builder.createTopology());
      }
   }
}

