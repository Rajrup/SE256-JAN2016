import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;



//import org.apache.storm.shade.com.google.common.hash.BloomFilter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import backtype.storm.tuple.Fields;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BuildAcceptableUsersBloomBolt implements IRichBolt {
	// Puts each unique member_id into a HashMap and counts their occurrence - will be useful in accurate filtering
	private Map<Long, Integer> acceptableMemberMap;
	
	// Puts each unique member_id into a BloomFilter of specified size and fpp
	private BloomFilter acceptableMemberBloomFilter;
	
	// Files where the serialized copies of the acceptable members HashMap and BloomFilters are stored
	private String bloomFilterSerialized;
	private String counterMapSerialized;
	
	private OutputCollector collector;
	
	public BuildAcceptableUsersBloomBolt(String bloomFilterSerialized, String counterMapSerialized){
		this.bloomFilterSerialized = bloomFilterSerialized;
		this.counterMapSerialized = counterMapSerialized;		
	}

   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	   // Initialize the class members appropriately
	   this.acceptableMemberMap = new HashMap<Long, Integer>();
	   
	   // Need to create a funnel (byte stream) from the Long Member Id, implement a method 
	   // of the Funnel interface and pass it on to the BloomFilter object to convert
	   // our data from any type (Long) to a Byte Stream that would be hashed by the Bloom Filter
	   Funnel memberFunnel = new Funnel<Long>() {

			public void funnel(Long member_id, PrimitiveSink into) {
				into.putLong(member_id);
			}
		  };
	   this.acceptableMemberBloomFilter = BloomFilter.create(memberFunnel, 175000, 0.1); // We'll change this as required
	   
	   this.collector = collector;
   }

   public void execute(Tuple tuple) {
	   // Get the tuple, populate the HashMap and BloomFilter appropriately
	   Long member_id = tuple.getLong(0);
		
	   if(!acceptableMemberMap.containsKey(member_id))
	   {
		   acceptableMemberMap.put(member_id, 1);
		   acceptableMemberBloomFilter.put(member_id);
	   }
	   else
	   {
		   Integer c = acceptableMemberMap.get(member_id) + 1;
		   acceptableMemberMap.put(member_id, c);
	   }
		
	   collector.ack(tuple);
   }

   public void cleanup() {
      
	   // Print out the HashMap and the count of each member_id encountered
//	   for(Map.Entry<Long, Integer> entry : acceptableMemberMap.entrySet()){
//		   System.out.println(entry.getKey()+" : " + entry.getValue());
//	   }
	   //System.out.println("HashMap Size: " + acceptableMemberMap.size());
	   
	   // Write the HashMap and BloomFilter into the file, for use in the next stage
	   try
	   {		   	   
		   FileOutputStream fos = new FileOutputStream(bloomFilterSerialized);
		   acceptableMemberBloomFilter.writeTo(fos);
	       fos.close();
	       
		   fos = new FileOutputStream(counterMapSerialized);
		   ObjectOutputStream oos = new ObjectOutputStream(fos);
		   oos.writeObject(acceptableMemberMap);
		   oos.close();
	       fos.close();
	       
	   }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   }
   }

   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("member_id"));
   }
	
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}
