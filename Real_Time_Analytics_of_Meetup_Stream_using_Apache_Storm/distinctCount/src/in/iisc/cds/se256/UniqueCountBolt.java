package in.iisc.cds.se256;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.lang.Integer;

public class UniqueCountBolt implements IRichBolt{
	private OutputCollector collector;
	private int numFieldCounterBolts;
	private int [] uniqueCount;
	private int distinctCount = 0;

	private BufferedWriter file_output;
	private FileSystem fs;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;
		numFieldCounterBolts = Integer.parseInt(conf.get("numFieldCounterBolts").toString());
		uniqueCount = new int[numFieldCounterBolts];

		Configuration configuration = new Configuration();

		configuration.addResource(new Path(conf.get("core-site").toString()));
		configuration.addResource(new Path(conf.get("hdfs-site").toString()));

		try{
			fs = FileSystem.newInstance(configuration);
			Path pt = new Path("/SSDSProject/data/uniqueCount/UniqueCountBolt");

			if (fs.exists(pt)) { // delete output folder if already present
				fs.delete(pt, true);
			}

			file_output = new BufferedWriter(new OutputStreamWriter(fs.create(pt)));
		} catch(Exception e){
			throw new RuntimeException("Error Emitting tuple", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		try{
			uniqueCount[input.getInteger(0).intValue()] = input.getInteger(1).intValue();
			distinctCount = 0;
			for(int i = 0; i < uniqueCount.length; i++){
				distinctCount += uniqueCount[i];
			}
			
			file_output.append("UniqueCountBolt" + "\t" + "Unique Count is :\t" + distinctCount);
			file_output.newLine();
			System.out.println("Unique Count is :\t" + distinctCount);
		} catch (Exception e){
			throw new RuntimeException("Error processing tuple for count by Aggregator Bolt", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count"));
	}

	@Override
	public void cleanup() {
		try {
			file_output.close();
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}