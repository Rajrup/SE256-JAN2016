package in.iisc.cds.se256;

import java.util.*;
import java.lang.Integer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

import java.nio.charset.Charset;

import java.text.SimpleDateFormat;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FieldCountBolt implements IRichBolt{
	// private HashSet<String> set;
	private HashSet<Long> set;
	private OutputCollector collector;

	private int bucketParam;
	private int numBuckets;
	private int [] maxZeros;
	private int distinctCount;
	private int uniqueCountDF; // whether to find exact unique Count or use Durand-Flajolet Algorithm

	private BufferedWriter file_output1;
	private BufferedWriter file_output2;
	private int fieldCountBoltIdx;
	private FileSystem fs1;
	private FileSystem fs2;

	private String counterField;
	private boolean fieldIdxSet = false;
	private int fieldIdx;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		// this.set = new HashSet<String>();
		this.set = new HashSet<Long>();
		this.collector = collector;

		counterField = conf.get("counterField").toString();

		bucketParam = Integer.parseInt(conf.get("bucketParam").toString());
		numBuckets = 1 << bucketParam; // for 2^bucketParam
		maxZeros = new int[numBuckets];

		uniqueCountDF = Integer.parseInt(conf.get("uniqueCountDF").toString());

		fieldCountBoltIdx = context.getThisTaskIndex();

		Configuration configuration = new Configuration();

		configuration.addResource(new Path(conf.get("core-site").toString()));
		configuration.addResource(new Path(conf.get("hdfs-site").toString()));

		try{
			fs1 = FileSystem.newInstance(configuration);
			Path pt = new Path("/SSDSProject/data/uniqueCount/FieldCountBolt_"+fieldCountBoltIdx);

			if (fs1.exists(pt)) { // delete output folder if already present
				fs1.delete(pt, true);
			}

			file_output1 = new BufferedWriter(new OutputStreamWriter(fs1.create(pt)));

			fs2 = FileSystem.newInstance(configuration);
			pt = new Path("/SSDSProject/data/uniqueCount/FieldCountDisplay_"+fieldCountBoltIdx);

			if (fs2.exists(pt)) { // delete output folder if already present
				fs2.delete(pt, true);
			}

			file_output2 = new BufferedWriter(new OutputStreamWriter(fs2.create(pt)));
		} catch(Exception e){
			throw new RuntimeException("Error processing tuple for count by Bolts", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		if(fieldIdxSet == false){
			fieldIdxSet = true;
			fieldIdx = input.fieldIndex(counterField);
		}

		long member_id = input.getLong(fieldIdx).longValue();
		long msg_id = input.getLong(0).longValue();
		String start_time = input.getString(1);

		if(uniqueCountDF == 0){
			if(!set.contains(member_id)){
				set.add(member_id);
			}
			collector.ack(input);
			distinctCount = set.size();
		}
		else{
			distinctCount = uniqueCount(member_id);
		}
		
		String end_time = new SimpleDateFormat("yyyy,MM,dd,HH,mm,ss,SSS").format(new Date());

		try{
			collector.emit(new Values(new Integer(fieldCountBoltIdx), new Integer(distinctCount)));

			file_output1.append("FieldCountBolt_" + fieldCountBoltIdx + "," + msg_id + "," + start_time + "," + end_time);
			file_output1.newLine();
			file_output2.append("FieldCountBolt_" + fieldCountBoltIdx + "\t" + "Distinct Count is :\t" + distinctCount);
			file_output2.newLine();
			System.out.println("FieldCountBolt_" + fieldCountBoltIdx + "," + msg_id + "," + start_time + "," + end_time);
			System.out.println("FieldCountBolt_" + fieldCountBoltIdx + "\t" + "Distinct Count is :\t" + distinctCount);
		} catch (Exception e){
			throw new RuntimeException("Error Emitting tuple", e);
		}
	}


	private int trailZeros(int n){
		if(n == 0)
			return 32;
		int bit_indx = 0;
		while( ((n >> bit_indx) & 1) == 0 ){
			bit_indx++;
		}
		return bit_indx;
	}

	private int uniqueCount(String str){
		//Based on Durand-Flajolet Algorithm

		double magicNum = 0.79402; // Durand-Flajolet magic number statistically derived in their paper.

		CharSequence charSeq = str; 

		HashFunction hf = Hashing.sha1();
		HashCode hc = hf.hashString(charSeq, Charset.defaultCharset());
		int hashVal = hc.asInt();

		int bucketId = hashVal & (numBuckets - 1);
		int modifHashVal = hashVal >> bucketParam;
		maxZeros[bucketId] = Math.max(maxZeros[bucketId], trailZeros(modifHashVal));

		double avgMaxZeros = 0;
		for(int i = 0; i < numBuckets; i++){
			avgMaxZeros += maxZeros[i];
		}
		avgMaxZeros /= numBuckets;

		return (int) (Math.pow(2, avgMaxZeros) * numBuckets * magicNum);
	}

	private int uniqueCount(int n){
		//Based on David-Flajolet Algorithm

		double magicNum = 0.79402; // Durand-Flajolet magic number statistically derived in their paper.

		HashFunction hf = Hashing.sha1();
		HashCode hc = hf.hashInt(n);
		int hashVal = hc.asInt();

		int bucketId = hashVal & (numBuckets - 1);
		int modifHashVal = hashVal >> bucketParam;
		maxZeros[bucketId] = Math.max(maxZeros[bucketId], trailZeros(modifHashVal));

		double avgMaxZeros = 0;
		for(int i = 0; i < numBuckets; i++){
			avgMaxZeros += maxZeros[i];
		}
		avgMaxZeros /= numBuckets;

		return (int) (Math.pow(2, avgMaxZeros) * numBuckets * magicNum);
	}

	private int uniqueCount(long l){
		//Based on David-Flajolet Algorithm

		double magicNum = 0.79402; // Durand-Flajolet magic number statistically derived in their paper.

		HashFunction hf = Hashing.sha1();
		HashCode hc = hf.hashLong(l);
		int hashVal = hc.asInt();

		int bucketId = hashVal & (numBuckets - 1);
		int modifHashVal = hashVal >> bucketParam;
		maxZeros[bucketId] = Math.max(maxZeros[bucketId], trailZeros(modifHashVal));

		double avgMaxZeros = 0;
		for(int i = 0; i < numBuckets; i++){
			avgMaxZeros += maxZeros[i];
		}
		avgMaxZeros /= numBuckets;

		return (int) (Math.pow(2, avgMaxZeros) * numBuckets * magicNum);
	}

	@Override
	public void cleanup() {
		try {
			file_output1.close();
			fs1.close();
			file_output2.close();
			fs2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("fieldCountBoltIdx","uniqueCount"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}