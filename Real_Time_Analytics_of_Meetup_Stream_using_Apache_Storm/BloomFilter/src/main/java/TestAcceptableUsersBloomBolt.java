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

public class TestAcceptableUsersBloomBolt implements IRichBolt {
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
	private Long membersHandledCount;
	private Long hashMapPresentCount;
	private Long hashMapAbsentCount;
	private Long bloomFilterMayPresentCount;
	private Long bloomFilterAbsentCount;
	private int recordsProcessed;
	private OutputCollector collector;
	private int idx;
	

	public TestAcceptableUsersBloomBolt(String bloomFilterSerialized, String counterMapSerialized){
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
	       this.acceptableMemberBloomFilter = BloomFilter.create(memberFunnel, 35000, 0.5);
	       acceptableMemberBloomFilter = acceptableMemberBloomFilter.readFrom(fis, memberFunnel);
	   }
	   catch(IOException e)
	   {
		   System.out.println("I/O Exception while reading in BloomFilter");
		   e.printStackTrace();
		   return;
	   }

	   // Initialize the counters
	   this.hashMapPresentCount = 0L;
	   this.hashMapAbsentCount = 0L;
	   this.bloomFilterMayPresentCount = 0L;
	   this.bloomFilterAbsentCount = 0L;
	   
	   this.testDataMap = new HashMap<Long, Integer>();

	   this.collector = collector;
   }

//   @Override
   public void execute(Tuple tuple) {
	   // Get the test tuple, populate the testData HashMap
	   // Check for its existance in the acceptable members HashMap and Bloomfilter respectively
	   // and update appropriate counters
	   Long member_id = tuple.getLong(0);
	   recordsProcessed++;
	   if(!testDataMap.containsKey(member_id))
	   {
		   testDataMap.put(member_id, 1);
		   
		   if(acceptableMemberMap.containsKey(member_id))
		   {
			   hashMapPresentCount++;
		   }
		   else
		   {
			   hashMapAbsentCount++;
		   }
		   
		   if(acceptableMemberBloomFilter.mightContain(member_id))
		   {
			   bloomFilterMayPresentCount++;
		   }
		   else
		   {
			   bloomFilterAbsentCount++;
		   }
	   }
	   else
	   {
		   Integer c = testDataMap.get(member_id) + 1;
		   testDataMap.put(member_id, c);
	   }

	   collector.ack(tuple);
   }

//   @Override
   public void cleanup() {
      
	   // Print out the HashMap and the count of each member_id encountered
//	   for(Map.Entry<Long, Integer> entry : acceptableMemberMap.entrySet()){
//		   System.out.println(entry.getKey()+" : " + entry.getValue());
//	   }
	   
	   // Print out the counters that help identify the discrepancy between exact filtering
	   // of HashMap and approximate filtering of BloomFilter
	   System.out.println("Hash Map Present Count: " + hashMapPresentCount);
	   System.out.println("Hash Map Absent Count: " + hashMapAbsentCount);
	   System.out.println("Bloom Filter Present Count: " + bloomFilterMayPresentCount);
	   System.out.println("Bloom Filter Absent Count: " + bloomFilterAbsentCount);
	   String outputFile = this.outputFolder + "/bolt-" + this.idx + ".txt";
	   
	   try {
		   
		    BufferedWriter bw= new BufferedWriter(new FileWriter(outputFile, false));		    
			
			String line = "Total tuples processed: " + recordsProcessed;
			bw.write(line);
			bw.write("\n");
			
			line = "Hash Map Present Count: " + hashMapPresentCount;
			bw.write(line);
			bw.write("\n");
			
			line = "Hash Map Absent Count: " + hashMapAbsentCount;
			bw.write(line);
			bw.write("\n");
			
			line = "Bloom Filter Present Count: " + bloomFilterMayPresentCount;
			bw.write(line);
			bw.write("\n");
			
			line = "Bloom Filter Absent Count: " + bloomFilterAbsentCount;
			bw.write(line);
			bw.write("\n");
			
			bw.flush();
			bw.close();
			
		} catch (IOException e) {
			System.out.println("Unable to create output file.");
			e.printStackTrace();
		}
   }

   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("member_id"));
   }
	
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}
