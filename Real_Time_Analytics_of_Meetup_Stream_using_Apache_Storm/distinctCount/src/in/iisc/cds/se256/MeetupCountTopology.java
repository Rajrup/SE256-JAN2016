package in.iisc.cds.se256;

import in.iisc.cds.se256.UniqueCountBolt;
import in.iisc.cds.se256.FieldFilterBolt;
import in.iisc.cds.se256.FieldCountBolt;
import in.iisc.cds.se256.StreamGenSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import backtype.storm.topology.BoltDeclarer;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.lang.Integer;
import backtype.storm.tuple.Fields;

public class MeetupCountTopology {

	public static void main(String[] args) throws Exception{

		String coreSitePath = (args.length > 0) ? args[0] : "/usr/hdp/2.2.9.1-10/hadoop/etc/hadoop/core-site.xml";
		String hdfsSitePath = (args.length > 1) ? args[1] : "/usr/hdp/2.2.9.1-10/hadoop/etc/hadoop/hdfs-site.xml";
		String fileListPath = (args.length > 2) ? args[2] : "/SSDSProject/data/FilePath";
		int filesPerSpout = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
		int spoutPerBolt = (args.length > 4) ? Integer.parseInt(args[4]) : 1;
		String fieldFilter = (args.length > 5) ? args[5] : "group_country";
		String filterWord = (args.length > 6) ? args[6] : "us";
		String counterField = (args.length > 7) ? args[7] : "member_id";
		int cluster_mode = (args.length > 8) ? Integer.parseInt(args[8]) : 0;
		int sleep_dur = Integer.parseInt((args.length > 9) ? args[9] : "500");
		String topology_name = (args.length > 10) ? args[10] : "Meetup stream Unique Count analysis";
		String bucketParam = (args.length > 11) ? args[11] : "10";
		String uniqueCountDF = (args.length > 12) ?  args[12] : "1";
		String notFilter = (args.length > 13) ?  args[13] : "0";
		int numFilterBolts = Integer.parseInt( (args.length > 14) ?  args[14] : "0" );
		int numFieldCounterBolts = Integer.parseInt( (args.length > 15) ?  args[15] : "0" );
		int numWorkers = Integer.parseInt( (args.length > 16) ?  args[16] : "1" );

		Config config = new Config();
		config.put("core-site", coreSitePath);
		config.put("hdfs-site", hdfsSitePath);
		config.put("filesPerSpout", String.valueOf(filesPerSpout));
		config.put("fieldFilter", fieldFilter);
		config.put("filterWord", filterWord);
		config.put("counterField", counterField);
		config.put("sleep_dur", String.valueOf(sleep_dur));
		config.put("bucketParam", bucketParam);
		config.put("uniqueCountDF", uniqueCountDF);
		config.put("notFilter", notFilter);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		Configuration configuration = new Configuration();

		configuration.addResource(new Path(coreSitePath));
		configuration.addResource(new Path(hdfsSitePath));
		FileSystem fs = FileSystem.newInstance(configuration);
		Path pt = new Path(fileListPath);

		String str;
		int file_count = 0;
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));;
		while ((str = reader.readLine()) != null) {
			config.put("fileNo_"+file_count, str);
			file_count++;
		}
		reader.close();
		fs.close();

		config.put("numFiles", String.valueOf(file_count));
		int numSpouts = (int) Math.ceil( (1.0*file_count) / filesPerSpout );

		// 1000 considered as baseline for sleep duration		
		// numFilterBolts = (numFilterBolts == 0) ? numSpouts * 1000 / sleep_dur : numFilterBolts;
		// numFieldCounterBolts = (numFieldCounterBolts == 0) ? (int) (1000 / sleep_dur * 
		// 	Math.ceil(numSpouts * 1.0/spoutPerBolt)) : numFieldCounterBolts;

		numFilterBolts = numSpouts * spoutPerBolt;
		numFieldCounterBolts = (int) Math.ceil( (numSpouts * 1.0) / spoutPerBolt);

		config.put("numSpouts", String.valueOf(numSpouts));
		config.put("numFieldCounterBolts", String.valueOf(numFieldCounterBolts));

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stream-gen-spout", new StreamGenSpout(), numSpouts);
		builder.setBolt("field-filter-bolt", new FieldFilterBolt(), numFilterBolts).shuffleGrouping("stream-gen-spout");
		builder.setBolt("field-counter", new FieldCountBolt(), 
			numFieldCounterBolts).fieldsGrouping("field-filter-bolt", new Fields(counterField));
		builder.setBolt("unique-counter", new UniqueCountBolt()).shuffleGrouping("field-counter");

		// numWorkers = (numWorkers == 0) ? 1 : numWorkers * 1000 / sleep_dur;
		numWorkers = numSpouts;

		config.setNumWorkers(numWorkers);
		System.out.println("Num Workers = " + numWorkers + " Num Spouts = " + numSpouts + 
			" Num Filter Bolts = " + numFilterBolts + " Num Field Count Bolt = " + numFieldCounterBolts);
		
		if(cluster_mode != 0){
			try{
				StormSubmitter.submitTopology(topology_name, config, builder.createTopology());
			} catch (AlreadyAliveException e){
				e.printStackTrace();
			} catch (InvalidTopologyException e){
				e.printStackTrace();
			}
		}
		else{
			LocalCluster local = new LocalCluster();
			local.submitTopology(topology_name, config, builder.createTopology());
			Thread.sleep(10000);
			Utils.sleep(10000);
			local.killTopology(topology_name);
			local.shutdown();
		}
	}

}