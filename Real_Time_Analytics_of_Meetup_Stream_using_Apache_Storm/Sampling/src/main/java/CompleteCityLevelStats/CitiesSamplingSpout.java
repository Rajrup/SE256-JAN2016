package CompleteCityLevelStats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class CitiesSamplingSpout extends BaseRichSpout {

// Random number generator
  private Random rnd;
  private String filePath;
  
  private FileReader fileReader;
  private BufferedReader reader;
  
  private SpoutOutputCollector collector;
  
  private int maxbucket;
  private int threshold;
  // For storing the list of words to be fed into the topology
  private HashSet<String> eventIds ;
  private String category_req ; 
  
public CitiesSamplingSpout(String filePath, int threshold) 
{
		super();
		this.filePath = filePath;
		this.threshold = threshold;
}

	//  @Override
  @SuppressWarnings("rawtypes")
 public void open( Map  map, TopologyContext   topologyContext,
      SpoutOutputCollector    spoutOutputCollector)
  {
    // initialize the random number generator
    rnd = new Random();
    
    maxbucket = 10;
    
    eventIds = new HashSet<String>();
    collector = spoutOutputCollector;
    
    category_req =  map.get("category").toString().trim();

    /* read file of rsvp Json  */
    fileReader = null;
		try 
		{
			fileReader = new FileReader(filePath);
			reader = new BufferedReader(fileReader);
		} 
		catch (Exception e)
		{
			System.out.println("Test : Exception in file reading" + e.getMessage());
		}
  }

//  @Override
  public void nextTuple()
  {
    // sleep a second before emitting any word
    //Utils.sleep(1);
    try 
    {
    	String event = null;
    	/* fetch the next tuple from the list */
    	if((event = reader.readLine())!=null)
      {
    		/* Parse the tuple into Json */
    		JSONParser jsonParser = new JSONParser();
				JSONObject eventObject  = (JSONObject) jsonParser.parse(event);
				JSONObject groupObject = (JSONObject) eventObject.get("group");
				String event_id = (String) eventObject.get("id");
				String location = (String)groupObject.get("city") ;
	    	String pay = null;
	    	Boolean isPaid = false, isTech = false;
				/* check for repetition 
	    	 * If we have already included this event in our sampling. If so do not consider this for sampling*/
	    	if(!eventIds.contains(event_id))	
				{	    	
	    	/* Check for sampling */
		    	int nextInt = rnd.nextInt(maxbucket);
	    		if(eventObject.get("payment_required") != null)
					{
						pay  = (String) eventObject.get("payment_required");
					}	
					if(pay.trim().equals("0"))
						isPaid = false;
					else 
						isPaid = true;
					JSONObject categoryObj  = (JSONObject) groupObject.get("category");
					String category = null;
					if(categoryObj != null)
						category = (String)categoryObj.get("name");
					
					if(category!=null && category.trim().equals(category_req))
						isTech = true;
					
	    		eventIds.add(event_id);
		    	if(nextInt < threshold )
		    	{
		    		collector.emit(new Values(event_id, location, isPaid, isTech, 1));
				}
		    	else
		    	{
		    		collector.emit(new Values(event_id, location, isPaid, isTech, 0));
		    	}
    	}
    	}
    }
    catch(Exception e)
    {
    	System.out.println("Test cities spout exception ");
    	e.printStackTrace();
    }
  }

//  @Override
  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("eventid", "location", "isPaid", "isTech", "isPartOfSample") );
  }

	@Override
	public void close() 
	{
		try 
		{
			fileReader.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
  
  
  
}

