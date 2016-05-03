import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


import org.apache.log4j.Logger;
import twitter4j.Status;

public class PrintBolt extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(PrintBolt.class);
	OutputCollector collector;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		 this.collector=collector;
	}
	public void execute(Tuple input) {
		Status status=(Status) input.getValueByField("status");
		LOG.info("Tweet is: "+status.getText());
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}
}
