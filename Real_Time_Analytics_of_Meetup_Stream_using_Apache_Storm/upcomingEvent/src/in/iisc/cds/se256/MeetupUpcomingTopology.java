package in.iisc.cds.se256;

import in.iisc.cds.se256.UpcomingBolt;
import in.iisc.cds.se256.FieldFilterBolt;
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

public class MeetupUpcomingTopology {

	public static void main(String[] args) throws Exception{

		// String coreSitePath = (args.length > 0) ? args[0] : "/home/stien/Programs/hadoop-2.6.3/etc/hadoop/core-site.xml";
		// String hdfsSitePath = (args.length > 1) ? args[1] : "/home/stien/Programs/hadoop-2.6.3/etc/hadoop/hdfs-site.xml";
		// String fileListPath = (args.length > 2) ? args[2] : "/input2/FilePath";
		String coreSitePath = (args.length > 0) ? args[0] : "/usr/hdp/2.2.9.1-10/hadoop/etc/hadoop/core-site.xml";
		String hdfsSitePath = (args.length > 1) ? args[1] : "/usr/hdp/2.2.9.1-10/hadoop/etc/hadoop/hdfs-site.xml";
		String fileListPath = (args.length > 2) ? args[2] : "/SSDSProject/data/FilePath_2";
		int filesPerSpout = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
		int spoutPerBolt = (args.length > 4) ? Integer.parseInt(args[4]) : 1;
		String fieldFilter_1 = (args.length > 5) ? args[5] : "status";
		String filterWord_1 = (args.length > 6) ? args[6] : "upcoming";
		String fieldFilter_2 = (args.length > 7) ? args[7] : "country";
		String filterWord_2 = (args.length > 8) ? args[8] : "us";
		String date = (args.length > 9) ? args[9] : "2016,04,18";
		int cluster_mode = (args.length > 10) ? Integer.parseInt(args[10]) : 0;
		String sleep_dur = (args.length > 11) ? args[11] : "500";
		String topology_name = (args.length > 12) ? args[12] : "Meetup stream Upcoming Event analysis";

		Config config = new Config();
		config.put("core-site", coreSitePath);
		config.put("hdfs-site", hdfsSitePath);
		config.put("filesPerSpout", String.valueOf(filesPerSpout));
		config.put("fieldFilter_1", fieldFilter_1);
		config.put("filterWord_1", filterWord_1);
		config.put("fieldFilter_2", fieldFilter_2);
		config.put("filterWord_2", filterWord_2);
		config.put("date", date);
		config.put("sleep_dur", sleep_dur);
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
		int numFilterBolts_2 = (int) (Math.ceil(numSpouts * 1.0 / 2));
		config.put("numSpouts", String.valueOf(numSpouts));

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stream-gen-spout", new StreamGenSpout(), numSpouts);
		builder.setBolt("field-filter-bolt-1", new FieldFilterBolt(1), numSpouts).shuffleGrouping("stream-gen-spout");
		builder.setBolt("field-filter-bolt-2", new FieldFilterBolt(2), 
			numFilterBolts_2).fieldsGrouping("field-filter-bolt-1", new Fields("event_name"));
		builder.setBolt("upcoming-bolt", new UpcomingBolt()).shuffleGrouping("field-filter-bolt-2");

		config.setNumWorkers(numSpouts);
		
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