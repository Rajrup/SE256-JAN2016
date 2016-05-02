import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class CountryNameBolt extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(CountryNameBolt.class);
	private OutputCollector collector;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
	}
	public void execute(Tuple input) {
		int sentiment=input.getIntegerByField("sentiment");
		String timeZone=input.getStringByField("timezone");
		String country=TwitterTimezones.countries.get(timeZone);
		if(country!=null)
			collector.emit(new Values(sentiment,country));
		//LOG.info("Country of Timezone "+timeZone+" is "+country);
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentiment","country"));
	}
}
