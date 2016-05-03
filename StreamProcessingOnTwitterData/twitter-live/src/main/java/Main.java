import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class Main {
	public static void main(String[] args) {
		new Main().run(args);
	}
	private void run(String[] args) {
		//TwitterSpout spout=new TwitterSpout();
		TwitterFileSpout spout=new TwitterFileSpout(args[0]);
		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout", spout, 1);
//		builder.setBolt("senti", new SentimentAnalysisBolt(),16).shuffleGrouping("spout");
//		builder.setBolt("country", new CountryNameBolt(),1).shuffleGrouping("senti");
//		builder.setBolt("sentimentCount", new SentimentUpdateBolt(),1).fieldsGrouping("country", new Fields("country"));
//		builder.setBolt("cassandra", new CassandraBolt(),1).shuffleGrouping("sentimentCount");
//		builder.setSpout("spout", new NormalFileSpout(args[0]), 1);
//		builder.setBolt("senti", new NormalSentimentAnalysisBolt(),2).shuffleGrouping("spout");
//		builder.setBolt("country", new CountryNameBolt(),2).shuffleGrouping("senti");
//		builder.setBolt("sentimentCount", new SentimentUpdateBolt(),2).fieldsGrouping("country", new Fields("country"));
//		builder.setBolt("cassandra", new CassandraBolt(),1).shuffleGrouping("sentimentCount");
		
		
		builder.setSpout("spout", spout, 1);
		builder.setBolt("senti", new SentimentAnalysisBolt2(),1).shuffleGrouping("spout");
		builder.setBolt("country", new CountryNameBolt2(),1).shuffleGrouping("senti");
		builder.setBolt("sentimentCount", new SentimentUpdateBolt2(),1).fieldsGrouping("country", new Fields("country"));
		builder.setBolt("cassandra", new CassandraBolt2(),1).shuffleGrouping("sentimentCount");
		Config conf = new Config();
	    conf.setDebug(false);
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("test", conf, builder.createTopology());
	    Utils.sleep(1000000);
	    cluster.killTopology("test");
		cluster.shutdown();
	}
}
