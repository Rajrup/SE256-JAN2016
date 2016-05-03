import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

//import org.apache.storm.shade.com.google.common.hash.BloomFilter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class CheckAgainstBloomAndHashBolt implements IRichBolt {
	// Reads in the acceptable members HashMap from the other topology
	private Map<Long, Integer> acceptableMemberMap;

	// Puts each unique member_id from test data into a HashMap and counts their occurrence
	private Map<Long, Integer> testDataMap;
	
	// Reads in the acceptable members Bloom Filter from the other topology
	private BloomFilter acceptableMemberBloomFilter;
	
	// Files where the serialized copies of the acceptable members HashMap and BloomFilters are stored
	private String bloomFilterSerialized;
	private String counterMapSerialized;
	private String outputFolder;

	// Counters to keep track of exact and approximate correctness of filtering data
	private OutputCollector collector;
	private int idx;
	

	public CheckAgainstBloomAndHashBolt(String bloomFilterSerialized, String counterMapSerialized){
		this.bloomFilterSerialized = bloomFilterSerialized;
		this.counterMapSerialized = counterMapSerialized;	
		this.outputFolder = outputFolder;
	}
	
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	   // Read in the acceptable members HashMap from serialized file
	   this.idx = context.getThisTaskId();
	   try
	   {
		   FileInputStream fis = new FileInputStream(counterMapSerialized);
	       ObjectInputStream ois = new ObjectInputStream(fis);
	       this.acceptableMemberMap = (HashMap) ois.readObject();
	       ois.close();
	       fis.close();
	       System.out.println("Test Size of hash map = " + this.acceptableMemberMap.size());
	   }
	   catch(IOException e)
	   {
		   System.out.println("I/O Exception while reading in HashMap");
		   e.printStackTrace();
	    	return;
	   }
	   catch(ClassNotFoundException e)
	   {
	   	   System.out.println("Class not found while reading in HashMap");
	   	e.printStackTrace();
	   	   return;
	   }	   

	   // Read in the acceptable members BloomFilter from serialized file
	   try
	   {
		   FileInputStream fis = new FileInputStream(bloomFilterSerialized);

	       Funnel memberFunnel = new Funnel<Long>() {

	    	   public void funnel(Long member_id, PrimitiveSink into) {
	    		   into.putLong(member_id);
	    	   }
	       };
	       this.acceptableMemberBloomFilter = BloomFilter.create(memberFunnel, 175000, 0.1);
	       acceptableMemberBloomFilter = acceptableMemberBloomFilter.readFrom(fis, memberFunnel);
	   }
	   catch(IOException e)
	   {
		   System.out.println("I/O Exception while reading in BloomFilter");
		   e.printStackTrace();
		   return;
	   }

	   this.testDataMap = new HashMap<Long, Integer>();

	   this.collector = collector;
   }

//   @Override
   public void execute(Tuple tuple) {
	   // Get the test tuple, populate the testData HashMap
	   // Check for its existance in the acceptable members HashMap and Bloomfilter respectively
	   // and update appropriate flags
	   Long member_id = tuple.getLong(0);
	   
	   Boolean isPresentInHash = false;
	   Boolean maybePresentInBloom = false;
	   
	   if(!testDataMap.containsKey(member_id))
	   {
		   testDataMap.put(member_id, 1);
		   
		   if(acceptableMemberMap.containsKey(member_id))
		   {
			   isPresentInHash = true;
		   }
		   else
		   {
			   isPresentInHash = false;
		   }
		   if(acceptableMemberBloomFilter.mightContain(member_id))
		   {
			   maybePresentInBloom = true;
		   }
		   else
		   {
			   maybePresentInBloom = false;
		   }
	   }
	   else
	   {
		   Integer c = testDataMap.get(member_id) + 1;
		   testDataMap.put(member_id, c);
	   }

	   collector.emit(new Values(member_id, isPresentInHash, maybePresentInBloom));
	   collector.ack(tuple);
   }

//   @Override
   public void cleanup() {
     
   }

   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("member_id", "isPresentInHash", "maybePresentInBloom"));
   }
	
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}
