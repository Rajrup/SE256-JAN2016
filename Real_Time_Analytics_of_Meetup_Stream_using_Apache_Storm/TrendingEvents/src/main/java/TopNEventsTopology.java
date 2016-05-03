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
 * args[1] = is cluster
 *args[2] = filpath
 */
public class TopNEventsTopology {

	public static void main(String[] args) throws Exception 
	{
		
		final int topN = 8;
		String toponame = args.length > 0 ? args[0] : "trending-events-topo";
		String isCluster = args.length > 1 ? args[1] : "L";
		String properties = args.length > 2 ? args[2] : "trending-events-properties.json";
		
		TopologyBuilder builder = new TopologyBuilder();	
		
		Config conf = new Config();		
		conf.setDebug(false);		
		conf.setMaxTaskParallelism(3);
		
		JSONParser parser = new JSONParser();
		JSONObject json =  (JSONObject) parser.parse(new String(Files.readAllBytes(Paths.get(properties))));
		
		String inputFilePath = (String) json.get("rsvp-spout-input-file-path");
		int spoutTasks = Integer.parseInt((String) json.get("rsvp-spout-tasks-count"));
		int spoutSleep = Integer.parseInt((String) json.get("rsvp-spout-sleep-time"));
		builder.setSpout("rsvp-spout", new RsvpSpout(inputFilePath, spoutSleep), spoutTasks);

		// RsvpCountBolt takes emitFrequency as an argument.
		int rsvpCountBoltEmitFreq = Integer.parseInt((String) json.get("count-bolt-emit-freq"));
		int rsvpCountBoltTasks = Integer.parseInt((String) json.get("count-bolt-tasks-count"));
		builder.setBolt("rsvp-count-bolt", new RsvpCountBolt(rsvpCountBoltEmitFreq), rsvpCountBoltTasks)
												.fieldsGrouping("rsvp-spout", new Fields("event_id"));
		
		// AggregationBolt takes emitFrequency as an argument.
		int aggBoltEmitFreq = Integer.parseInt((String) json.get("agg-bolt-emit-freq"));
		int aggBoltTasks = Integer.parseInt((String) json.get("agg-bolt-tasks-count"));
		int slidingWindowSize = Integer.parseInt((String) json.get("sliding-window-size"));
		builder.setBolt("aggs-bolt", new AggregationBolt(aggBoltEmitFreq, slidingWindowSize), aggBoltTasks)
												.fieldsGrouping("rsvp-count-bolt", new Fields("event_id"));
		
		// TotalRankingBolt takes emitFrequency and topN as arguments.
		int rankBoltEmitFreq = Integer.parseInt((String) json.get("rank-bolt-emit-freq"));
		int rankBoltTasks = Integer.parseInt((String) json.get("rank-bolt-tasks-count"));
		builder.setBolt("total-rank-bolt", new TotalRankingBolt(rankBoltEmitFreq, topN), rankBoltTasks).globalGrouping("aggs-bolt");
		
		// ReportBolt takes output file path as arguments.
		int reportBoltTasks = Integer.parseInt((String) json.get("rank-bolt-tasks-count"));
		String outputFilePath = (String) json.get("report-bolt-output-file-path");
		builder.setBolt("report-bolt", new ReportBolt(outputFilePath), reportBoltTasks).globalGrouping("total-rank-bolt");
		
		if (isCluster.trim().equals("L")) 
		{  
			LocalCluster cluster = new LocalCluster();     
			cluster.submitTopology("topN-popular-events", conf, builder.createTopology());		      
			Thread.sleep(180000);
			cluster.shutdown();
			System.exit(0);
		}
		else
		{
			conf.setNumWorkers(3);
			conf.registerSerialization(SlidingWindow.class);
			StormSubmitter.submitTopology(toponame, conf, builder.createTopology());
		}
	}
}
