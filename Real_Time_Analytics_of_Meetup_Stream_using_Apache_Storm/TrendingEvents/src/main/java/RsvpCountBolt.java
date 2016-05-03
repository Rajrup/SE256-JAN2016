import java.util.HashMap;
import java.util.Iterator;
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


public class RsvpCountBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 4228401031661813685L;
	
	private OutputCollector collector;
	private Map<String, Integer> countMap;	
	private final int emitFrequencySeconds;
	
	
	public RsvpCountBolt(int emitFrequencySeconds){
		this.emitFrequencySeconds = emitFrequencySeconds; 
	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		collector = oc;
		countMap = new HashMap<String, Integer>();
	}
	
	public void execute(Tuple tuple) {
		
		try{
			
			if(Utils.isTickTuple(tuple)){				
				emitCurrentCounts();				
			}
			else{
				
				String event_id = tuple.getStringByField("event_id");
				String response = tuple.getStringByField("response");
				if(response.equals("yes"))
				{
					if(countMap.containsKey(event_id)){
						int count = countMap.get(event_id);					
						countMap.put(event_id, ++count);
					}
					else{
						countMap.put(event_id, 1);
					}					
				}				
			}
		}
		catch(Exception e){
			System.out.println("Exception occurred in RsvpCountBolt execute()");
			e.printStackTrace();
		}
		
	}
	
	private void emitCurrentCounts(){
		
		Iterator<Entry<String, Integer>> iter = this.countMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, Integer> entry = iter.next();
			collector.emit(new Values(entry.getKey(), entry.getValue()));
			entry.setValue(0);
		}
		this.countMap = new HashMap<String, Integer>();
	}
		

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event_id", "count"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencySeconds);
        return conf;
	}

}
