package CompleteCityLevelStats;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class CitiesSamplingTopology 
{
	public static void main(String[] args) throws Exception
	{
		String toponame = args.length > 0 ? args[0] : "sampling-exact-topo";
		String isCluster = args.length > 1 ? args[1] : "L";
		String properties = args.length > 2 ? args[2] : "sampling-properties.json";	
		
		TopologyBuilder builder = new TopologyBuilder();
		
		JSONParser parser = new JSONParser();
		JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
		
		String inputFilePath = (String) json.get("input-file-path");
		int countTasks = Integer.parseInt((String) json.get("count-tasks"));
		String category = (String) json.get("category");
		String outputFilePathSampling = (String) json.get("output-file-path-samples");
		String outputFilePathResults = (String) json.get("output-file-path-results");
		int samplingRate = Integer.parseInt((String) json.get("sampling-rate"));
		
    builder.setSpout("cities-spout", new CitiesSamplingSpout(inputFilePath, samplingRate), 1);
    
    // attach the count bolt using fields grouping 
    builder.setBolt("citiescount-bolt", new CitiesCountBolt(), countTasks)
    			.fieldsGrouping("cities-spout", new Fields("location"));
   
    builder.setBolt("report-bolt", new CitiesReportBolt(outputFilePathSampling, outputFilePathResults), 1)
        					.globalGrouping("citiescount-bolt");
    
    Config conf = new Config();
    conf.setDebug(true);    
    conf.put("category", category);
    conf.put("toponame", toponame);
    
    /* running on cluster */
    if (isCluster.trim().equals("C")) 
    {
      conf.setNumWorkers(3);      
      conf.registerSerialization( CountStats.class);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } 
    /* running locally */
    else 
    {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology(toponame, conf, builder.createTopology());
      Thread.sleep(30000);
      cluster.shutdown();
    }
  }
}
