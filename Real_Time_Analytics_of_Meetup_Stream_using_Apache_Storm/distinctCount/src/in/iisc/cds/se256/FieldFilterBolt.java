package in.iisc.cds.se256;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.lang.Integer;

public class FieldFilterBolt implements IRichBolt{
	private OutputCollector collector;
	private String fieldFilter, filterWord;
	private boolean fieldFilterSet = false;
	private int fieldIdx;
	private int filterCount = 0;
	private int totalCount = 0;
	private int filterBoltIdx;

	private int notFilter = 0;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		fieldFilter = conf.get("fieldFilter").toString();
		filterWord = conf.get("filterWord").toString();

		notFilter = Integer.parseInt(conf.get("notFilter").toString());

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
			if(filterWord.equals((input.getString(fieldIdx).toString())) || notFilter == 1) {
				filterCount++;
				totalCount++;

				collector.emit(new Values(input.getLongByField("tuple_id"), input.getStringByField("time"), 
					input.getLongByField("group_id"), input.getStringByField("group_country"), 
					input.getStringByField("group_city"), input.getStringByField("event_name"), 
					input.getStringByField("event_id"), input.getLongByField("event_time"), 
					input.getLongByField("member_id")));

				System.out.println("FilterBolt_" + filterBoltIdx + " filtered " + 
					filterCount + " out of " + totalCount + " tuple.");
			}
			else{
				totalCount++;
				System.out.println("FilterBolt_" + filterBoltIdx + " filtered " + 
					filterCount + " out of " + totalCount + " tuple.");
			}
		} catch (Exception e){
			throw new RuntimeException("Error filetring tuple", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tuple_id", "time", "group_id", "group_country", 
			"group_city", "event_name", "event_id", "event_time", "member_id"));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}