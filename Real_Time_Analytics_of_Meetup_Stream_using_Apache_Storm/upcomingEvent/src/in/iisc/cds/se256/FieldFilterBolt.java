package in.iisc.cds.se256;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FieldFilterBolt implements IRichBolt{
	private OutputCollector collector;
	private int level;

	private String fieldFilter, filterWord;
	private boolean fieldFilterSet = false;
	private int fieldIdx;
	private int filterCount = 0;
	private int totalCount = 0;
	private int forwardedCount = 0;
	private int filterBoltIdx;

	private HashSet<String> event_set = new HashSet<String>();

	public FieldFilterBolt(){
		this.level = 1;
	}

	public FieldFilterBolt(int level){
		this.level = level;
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		fieldFilter = conf.get("fieldFilter_" + level).toString();
		filterWord = conf.get("filterWord_" + level).toString();
		filterBoltIdx = context.getThisTaskIndex();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try{
			if(fieldFilterSet == false){
				fieldFilterSet = true;
				fieldIdx = input.fieldIndex(fieldFilter);
			}
			if(filterWord.equals((input.getString(fieldIdx).toString()))) {
				filterCount++;
				totalCount++;

				String event_name = input.getStringByField("event_name");
				if(level == 1 || (level == 2 && !event_set.contains(event_name))){
					forwardedCount++;
					if(level == 2){
						event_set.add(event_name);
					}
					collector.emit(new Values(input.getStringByField("tuple_id"), input.getStringByField("time"), 
						input.getStringByField("country"), input.getStringByField("city"), 
						input.getStringByField("event_name"), input.getLongByField("event_time"), 
						input.getStringByField("status")));
				}

				System.out.println("FilterBolt_" + filterBoltIdx + " Filter Level " + level + "filterWord " + 
					filterWord + " filtered " + filterCount + " out of " + totalCount + " tuple " + 
					", forwardedCount " + forwardedCount);
			}
			else{
				totalCount++;
				System.out.println("FilterBolt_" + filterBoltIdx + " Filter Level " + level + "filterWord " + 
					filterWord + " filtered " + filterCount + " out of " + totalCount + " tuple " + 
					", forwardedCount " + forwardedCount);
			}
		} catch (Exception e){
			throw new RuntimeException("Error filtering tuple", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple_id", "time", "country", 
						"city", "event_name", "event_time", "status"));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}