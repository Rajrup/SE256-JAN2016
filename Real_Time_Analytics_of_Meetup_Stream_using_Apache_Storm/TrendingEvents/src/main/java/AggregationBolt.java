import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class AggregationBolt extends BaseRichBolt {
		
	private static final long serialVersionUID = -594254859346937029L;
	
	private OutputCollector collector;
	private final int emitFrequencySeconds;
	private SlidingWindow slidingWindow;
	private int slidingWindowSize;
	
	public AggregationBolt(int emitFrequencySeconds, int slidingWindowSize){
		this.emitFrequencySeconds = emitFrequencySeconds;
		this.slidingWindowSize  = slidingWindowSize;
	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		collector = oc;
		try {
			slidingWindow = new SlidingWindow(this.slidingWindowSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		try{			
			if(Utils.isTickTuple(tuple)){				
				emitCurrentRankings();				
			}
			else{				
				String event_id = tuple.getStringByField("event_id");
				int ecount = tuple.getIntegerByField("count");
				slidingWindow.put(event_id, ecount);
			}			
		}
		catch(Exception e){
			System.out.println("Exception occurred in AggregationBolt execute()");
			e.printStackTrace();
		}
	}

	private void emitCurrentRankings(){
		HashMap<String, Integer> tempMap = slidingWindow.getAggregateMapAndAdvance();
		collector.emit(new Values(tempMap));
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
