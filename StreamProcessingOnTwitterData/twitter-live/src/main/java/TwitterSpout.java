import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout{
	private static final Logger LOG = Logger.getLogger(TwitterSpout.class);
	private static final String consumerKey="OTDyM1ql9bEYzSYvyfFyiJ6uu";
	private static final String consumerSecret="t4nZXYj0KQvaXGXjxUUg4DhjjE7uadv52Hg7H1ZXzY2AbpHcT9";
	private static final String accessToken="725542327222153216-7ndlobmjj7Oru672tZdjiiY830WMJLh";
	private static final String accessTokenSecret="axyjM9RYPyls0DSCZojp9EctdGibqdWCyDbZ3BY5lfoFM";
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	   private static final Logger log = Logger.getLogger(TwitterSpout.class);
	    // .getLogger(TwitterSpout.class);

	    SpoutOutputCollector _collector;
	    LinkedBlockingQueue<Status> queue = null;
	    long myRandomMsgId;

	    @Override
	    public void open(Map confMap, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
        queue = new LinkedBlockingQueue<Status>(1000);

        //implement a listener for twitter statuses
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }

            public void onScrubGeo(long userId, long upToStatusId) {
            }

            public void onStallWarning(StallWarning warning) {
            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey(consumerKey)
		  .setOAuthConsumerSecret(consumerSecret)
		  .setOAuthAccessToken(accessToken)
		  .setOAuthAccessTokenSecret(accessTokenSecret);
		TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
		TwitterStream twitterStream = tf.getInstance();
		twitterStream.addListener(listener);
//			FilterQuery query=new FilterQuery();
//			query.language(new String[]{"en"});
//			query.filterLevel("none");
//			query.track(tracks);
//			twitterStream.filter(query);
		twitterStream.sample();
    }
	    private long timeStart=0;
		private long timeStop=0;
		private boolean firstTime=true;
		private long count=0;
    @Override
    public void nextTuple() {

        Status ret =null;
        queue.poll();
        if (ret == null) {
            //if queue is empty sleep the spout thread so it doesn't consume resources
            Utils.sleep(50);
        } else {

         
          _collector.emit(new Values(ret));

            System.out.println(ret.getUser().getName() + " : " + ret.getText());
        }
    }



    @Override
    public void ack(Object id) {
        System.out.print("The ACK value of in TwitterSpout is " + id);
    }

    @Override
    public void fail(Object id) {
        System.out.print("The fail value due to database exception of in RandomSentenceSpout is " + id);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }


}
