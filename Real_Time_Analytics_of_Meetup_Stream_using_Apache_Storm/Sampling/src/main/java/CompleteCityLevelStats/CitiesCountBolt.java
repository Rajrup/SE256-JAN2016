package CompleteCityLevelStats;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CitiesCountBolt  extends BaseRichBolt 
{

// To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;
  
  // Map to store the count of the words
  private Map<String, CountStats> countMap;

//  @Override
  @SuppressWarnings("rawtypes")
public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;
    
    countMap = new HashMap<String, CountStats>();
  } 

//  @Override
  public void execute(Tuple tuple)
  {
    //Location count values 
  	CountStats cs ;
  	try 
    {
    	String location = tuple.getStringByField("location");
    	Boolean isPaid = tuple.getBooleanByField("isPaid");
	    Boolean isTech = tuple.getBooleanByField("isTech");
	    int isPartOfSample = tuple.getIntegerByField("isPartOfSample");
	    
    	if(countMap.containsKey(location))
	    {
	    	cs = countMap.get(location);
	    	cs.eventCount++;
	    	if(isPaid)
	    		cs.paidEventCount++;
	    	if(isTech)
	    		cs.techEventCount++;
	    	if(isPartOfSample == 1)
	    	{
		    	cs.sampledEventCount++;
		    	if(isPaid)
		    		cs.sampledPaidEventCount++;
		    	if(isTech)
		    		cs.sampledTechEventCount++;
	    	}
	    	countMap.put(location, cs);
	    }
	    else
	    {
	    	cs = new CountStats();
	    	cs.eventCount = 1;
	    	if(isPaid)
	    		cs.paidEventCount = 1;
	    	if(isTech)
	    		cs.techEventCount = 1;
	    	if(isPartOfSample == 1)
	    	{
		    	cs.sampledEventCount = 1;
		    	if(isPaid)
		    		cs.sampledPaidEventCount = 1;
		    	if(isTech)
		    		cs.sampledTechEventCount = 1;
	    	}
	    	countMap.put(location, cs);
	    }
	     //After countMap is updated, emit word and count to output collector
	    // Syntax to emit the word and count (uncomment to emit)
    		collector.emit(new Values(location, cs));
    }
    catch(Exception e)
    {
    	System.out.println("Test Count Bolt exception " +e.getMessage());
    }
  }

//  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("location", "statsObj"));
  }
  
  public void cleanup() 
	{
	}
}



