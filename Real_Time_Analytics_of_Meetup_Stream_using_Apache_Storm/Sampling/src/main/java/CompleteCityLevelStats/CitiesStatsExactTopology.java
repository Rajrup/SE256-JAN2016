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
/*args[0] = toponame
 * args[1] = L or C 
 *args[2] = filepath 
 * args[3] = category
*/
public class CitiesStatsExactTopology 
{
	public static void main(String[] args) throws Exception
  {		
		
		String toponame = args.length > 0 ? args[0] : "trending-events-topo";
		String isCluster = args.length > 1 ? args[1] : "L";
		String properties = args.length > 2 ? args[2] : "trending-events-properties.json";	
		
		TopologyBuilder builder = new TopologyBuilder();
		
		JSONParser parser = new JSONParser();
		JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
		
		String inputFilePath = (String) json.get("input-file-path");
		String countTasks = (String) json.get("count-tasks");
		String category = (String) json.get("category");
		String outputFilePath = (String) json.get("output-file-path");
		String logFilePath = (String) json.get("log-file-path");
		
    builder.setSpout("cities-spout", new CitiesCountExactSpout(inputFilePath, logFilePath), 1);
    
    builder.setBolt("citiescount-bolt", new CitiesCountBolt(), Integer.parseInt(countTasks)).fieldsGrouping("cities-spout", new Fields("location"));
    
    builder.setBolt("report-bolt", new CitiesReportBolt(outputFilePath, logFilePath), 1).globalGrouping("citiescount-bolt");
    
    Config conf = new Config();
    conf.setDebug(false);
    conf.setNumWorkers(3);
    conf.registerSerialization( CountStats.class);
  	conf.put("category", category);
  	conf.put("toponame", toponame);
  	conf.put("outputfile", outputFilePath);
  	
    if (isCluster.trim().equals("C") ) 
    {
    	// set the number of workers for running all spout and bolt tasks
      StormSubmitter.submitTopology(toponame, conf, builder.createTopology());
    } 
    else 
    {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology(toponame, conf, builder.createTopology());
      Thread.sleep(30000);
      cluster.shutdown();
    }
  }
}
