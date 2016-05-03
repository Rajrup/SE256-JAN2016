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

public class AcceptableUserReportBolt  extends BaseRichBolt
{
  private String outputFilePathResults;
  private int emitFrequencySeconds = 1;
  
  private int hashMemberCount;
  private int bloomMemberCount;
  
  	public AcceptableUserReportBolt(String outputFilePathResults ) {
		super();
		this.outputFilePathResults = outputFilePathResults;
	}
  
  @SuppressWarnings("rawtypes")
  	public void prepare(Map map, TopologyContext  topologyContext,  OutputCollector  outputCollector)
  {
	  hashMemberCount = 0;
	  bloomMemberCount = 0;
  }

	public void execute(Tuple tuple)
    {  	
		if(isTickTuple(tuple))
		{
			String results = "Members present in the Accurate HashMap: " + hashMemberCount + "\n"
					+ "Members present in the Bloom Filter: " + bloomMemberCount + "\n" 
					+ "Error Rate in Bloom Filter [(members in Bloom Filter - members in HashMap) / members in Bloom Filter]: " 
					+ (double)(bloomMemberCount - hashMemberCount) / bloomMemberCount + "\n";
			dumpDataToFile(outputFilePathResults, results);

		}
		else
		{
			Long member_id = tuple.getLongByField("member_id");
			Boolean isPresentInHash = tuple.getBooleanByField("isPresentInHash");
			Boolean maybePresentInBloom = tuple.getBooleanByField("maybePresentInBloom");
			
			if(isPresentInHash)
			{
				hashMemberCount++;
			}
			if(maybePresentInBloom)
			{
				bloomMemberCount++;
			}
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
}
