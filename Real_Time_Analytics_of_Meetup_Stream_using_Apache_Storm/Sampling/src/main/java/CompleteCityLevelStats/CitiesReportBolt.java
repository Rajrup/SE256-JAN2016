package CompleteCityLevelStats;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class CitiesReportBolt  extends BaseRichBolt
{
  private HashMap<String, CountStats> finalStats;
  private String outputFilePathSampling;
  private String outputFilePathResults;
  private int emitFrequencySeconds = 1;
  
  	public CitiesReportBolt(String outputFilePathSampling, String outputFilePathResults ) {
		super();
		this.outputFilePathSampling = outputFilePathSampling;
		this.outputFilePathResults = outputFilePathResults;
	}
  
  @SuppressWarnings("rawtypes")
  	public void prepare(Map map, TopologyContext  topologyContext,  OutputCollector  outputCollector)
  {
  	finalStats = new HashMap<String, CountStats>();
  }

	public void execute(Tuple tuple)
    {  	
		if(isTickTuple(tuple))
		{
			int overallEventCount = 0;
			int sampledEventCount = 0;
			int overallPaidEventCount = 0;
			int sampledPaidEventCount = 0;
			int overallTechEventCount = 0;
			int sampledTechEventCount = 0;
			
	   		for(Entry<String, CountStats> entryInner : finalStats.entrySet())
	   		{
	   			overallEventCount += entryInner.getValue().eventCount;
	   			sampledEventCount += entryInner.getValue().sampledEventCount;
	   			overallPaidEventCount += entryInner.getValue().paidEventCount;
	   			sampledPaidEventCount += entryInner.getValue().sampledPaidEventCount;
	   			overallTechEventCount += entryInner.getValue().techEventCount;
	   			sampledTechEventCount += entryInner.getValue().sampledTechEventCount;
	   		}
			
			String results = "Overall Event Count: " + overallEventCount + "\tSampled Event Count: " + sampledEventCount + "\n"
					+ "Overall Paid Event Count: " + overallPaidEventCount + "\tSampled Paid Event Count: " + sampledPaidEventCount + "\n" 
					+ "Overall Tech Event Count: " + overallTechEventCount + "\tSampled Tech Event Count: " + sampledTechEventCount + "\n";
			dumpDataToFile(outputFilePathResults, results);

			dumpDataToFile(outputFilePathSampling, finalStats);
		}
		else
		{
			String location = tuple.getStringByField("location");		  
			CountStats cs = (CountStats)tuple.getValueByField("statsObj"); 
			finalStats.put(location, cs);

		}
    }
  

  @Override
	public void cleanup() 
    {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // nothing to add - since it is the final bolt
    }
	
	public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencySeconds);
        return conf;
	}

	public void dumpDataToFile(String filepath, String data)
	{
		File file = new File(filepath);
		BufferedWriter outputResults;
		if(file.exists()){
			file.delete();
		}
		try {
			outputResults = new BufferedWriter(new FileWriter(filepath));
			outputResults.write(data);
			outputResults.flush();
			outputResults.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void dumpDataToFile(String filepath, HashMap<String, CountStats> finalStats)
	{
		File file = new File(filepath);
		BufferedWriter outputSamples;
		if(file.exists()){
			file.delete();
		}
		try {
			outputSamples = new BufferedWriter(new FileWriter(filepath));

			for(Entry<String, CountStats> entry : finalStats.entrySet()){
	  			CountStats cs = entry.getValue();
	  			String location = entry.getKey();
	  			outputSamples.write(location + "\t" + cs.eventCount +"\t" + cs.sampledEventCount +"\t"  
						+ cs.paidEventCount +"\t" + cs.sampledPaidEventCount +"\t" 
						+ cs.techEventCount +"\t" + + cs.sampledTechEventCount +"\n");
	  		}
			
			outputSamples.flush();
			outputSamples.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
