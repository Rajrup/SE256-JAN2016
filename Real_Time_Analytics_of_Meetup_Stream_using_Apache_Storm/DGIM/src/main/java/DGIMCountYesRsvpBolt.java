import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class DGIMCountYesRsvpBolt implements IRichBolt {
	
	private DGIMContainer dgim;
	
	private static final int WINDOW_SIZE = 14;
	private static final int MAX_SAME_SIZE_BUCKETS = 2;
	private static final int INTERVAL_SIZE = 13;	
	private int index;
	private String outputFile ;
	FileWriter fw;
	BufferedWriter bw;
	
	private OutputCollector collector;

	public DGIMCountYesRsvpBolt(String outputFile) {
		super();
		this.outputFile = outputFile;
	}
   
   public void prepare(Map conf, TopologyContext context, OutputCollector collector)
   {
	   if(INTERVAL_SIZE > WINDOW_SIZE)
	   {
		   System.out.println("DGIM Not applicable. Interval Size larger than Window size. Try again");
		   System.exit(-1);
	   }
	   this.dgim = new DGIMContainer(WINDOW_SIZE, MAX_SAME_SIZE_BUCKETS);
	   this.index = 0;
	   
	   try
	   {
		   fw = new FileWriter(outputFile);
		   bw = new BufferedWriter(fw);
	   }
	   catch(IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   }

	   this.collector = collector;
   }

   
   public void execute(Tuple tuple) {
	   int rsvp_response = tuple.getInteger(0);
	   dgim.consumeInputBit(rsvp_response);
	   int dgimCount = dgim.getCount(INTERVAL_SIZE);
	   int accurateCount = dgim.getAccurateCount(INTERVAL_SIZE);
	   
	   // collector.emit(new Values(index, dgimCount, accurateCount));
	   // System.out.println("Index: " + index + " DGIMCount: " + dgimCount + " ExactCount: " + accurateCount);

	   try
	   {
  			bw.write(index + "," + dgimCount + "," + accurateCount + "\n");
	   }
	   catch(IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   }

	   index = (index + 1) % WINDOW_SIZE; 
	   collector.ack(tuple);
   }

   
   public void cleanup() {
	   try
	   {
		   bw.close();
	       fw.close();
	   }
	   catch (IOException e)
	   {
    	   System.out.println("Could not open file: " + e.getMessage());
	   } 
   }

   
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("index", "dgimCount", "accurateCount"));
   }
	
   
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
}
