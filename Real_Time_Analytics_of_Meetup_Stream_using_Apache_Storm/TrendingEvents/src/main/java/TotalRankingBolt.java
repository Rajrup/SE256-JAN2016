import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TotalRankingBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 8693756123035271333L;
	
	private OutputCollector collector;
	private final int emitFrequencySeconds;
	private final int topN;
	private HashMap<String, Integer> countMap;
		
	public TotalRankingBolt(int emitFrequencySeconds, int topN){
		this.emitFrequencySeconds = emitFrequencySeconds;
		this.topN = topN;
	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		collector = oc;
		countMap = new HashMap<String, Integer>();
	}

	public void execute(Tuple tuple) {
		try{			
			if(Utils.isTickTuple(tuple)){				
				emitCurrentRankings();
			}
			else{
				@SuppressWarnings("unchecked")
				HashMap<String, Integer> tempMap = (HashMap<String, Integer>) tuple.getValueByField("rankings");
				mergeIntoCountMap(tempMap);
			}			
		}
		catch(Exception e){
			System.out.println("Exception occurred in TotalRankingBolt execute()");
			e.printStackTrace();
		}
	}

	private void emitCurrentRankings(){
		LinkedHashMap<String, Integer> tempMap = (LinkedHashMap<String, Integer>) Utils.sortByValue(countMap);
		LinkedHashMap<String, Integer> emitMap = new LinkedHashMap<String, Integer>();
		
		int index = 0;
		for(Entry<String, Integer> entry : tempMap.entrySet()){
			emitMap.put(entry.getKey(), entry.getValue());
			++index;
			if(index == topN)
				break;
		}
		collector.emit(new Values(emitMap));
		countMap = new HashMap<String, Integer>();
	}
	
	private void mergeIntoCountMap(HashMap<String, Integer> tempMap){
		for(Entry<String, Integer> entry : tempMap.entrySet())
		{
			if(countMap.containsKey(entry.getKey()))
			{
				int val = countMap.get(entry.getKey());
				val += entry.getValue();
				countMap.put(entry.getKey(), val);				
			}
			else {
				countMap.put(entry.getKey(), entry.getValue());
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rankings"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencySeconds);
        return conf;
	}

}
