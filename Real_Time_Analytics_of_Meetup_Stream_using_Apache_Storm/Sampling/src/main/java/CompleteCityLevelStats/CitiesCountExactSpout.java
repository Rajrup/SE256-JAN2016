package CompleteCityLevelStats;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.InputStreamReader;

public class CitiesCountExactSpout extends BaseRichSpout 
{
	private String filePath;
  private int listSize;
  private FileReader fileReader;
//  private BufferedReader reader;
  private int lineCounter;
  private static JSONParser jsonParser = new JSONParser();
  private SpoutOutputCollector collector;
  private FileWriter fileWriter;
  private BufferedWriter writer;
  private String category_req ;  
  private FileSystem fs;
  private Configuration configuration ;
  private int idx ;
  private BufferedReader reader; 
  private long starttime; 
  private String logFilePath;
  // For storing the list of words to be fed into the topology
  private HashSet eventIds ;

	public CitiesCountExactSpout(String filePath, String logFilePath)
	{
		super();
		this.filePath = filePath;
		this.logFilePath = logFilePath;
	}

	public void open( Map  map, TopologyContext   topologyContext,
      SpoutOutputCollector    spoutOutputCollector)
  {
    eventIds = new HashSet<String>();
    collector = spoutOutputCollector;
    lineCounter = 0;   
    fileReader = null;
    try 
		{
			
			fileReader = new FileReader(filePath);
			reader = new BufferedReader(fileReader);
			category_req =  (String)map.get("category").toString().trim();			
			idx = new Random().nextInt(10);
			String logfile = (String)map.get("toponame");
			
			fileWriter = new FileWriter(logFilePath+logfile+"_"+idx+".txt");
			writer = new BufferedWriter(fileWriter);
			starttime = System.currentTimeMillis();
		} 
		catch (Exception e)
		{
			System.out.println("Test : Exception in file reading" + e.getMessage());
		}
  }

  public void nextTuple()
  {
    Utils.sleep(1000);
    String event = null;
    try 
    {
    	event = reader.readLine();
    	if(event!=null)
      {
		    /* Parse the tuple into Json */
				lineCounter++;
				JSONObject eventObject  = (JSONObject) jsonParser.parse(event);
				JSONObject groupObject = (JSONObject) eventObject.get("group");
				String event_id = (String) eventObject.get("id");
				String location = (String)groupObject.get("city") ;
				String pay = null;
				boolean isPaid ;
				boolean isTech= false;
				/* check for repetition 
	    	 * If we have already included this event in our sampling. If so do not consider this for sampling*/    	
				if(!eventIds.contains(event_id))	
				{	
					if(eventObject.get("payment_required") != null)
					{
						pay  = (String) eventObject.get("payment_required");
					}
					if(pay.trim().equals("0"))
						isPaid = false;
					else 
						isPaid = true;
					JSONObject categoryObj  = (JSONObject) groupObject.get("category");
					String 	category = (String)categoryObj.get("shortname");
					
					if(category.trim().equals(category_req))
						isTech = true;
		
	    		eventIds.add(event_id);
	    		long st = System.currentTimeMillis();
	    		Date date = new Date();
	    		writer.write(event_id +","+ date.getHours()+"," + date.getMinutes() + "," +date.getSeconds()+"\n");
	    		collector.emit(new Values(event_id , location, isPaid , isTech , st ));
    	}
    	}
    	else
    	{
    		long diff = System.currentTimeMillis() - starttime;
    		writer.write("Total records given by spout "+lineCounter+"Total time take in ms "+diff +"\n");
    	}
    }
    catch(Exception e)
    {
    	System.out.println("Test cities spout exception " +e.getMessage());
    	e.getStackTrace();
    }
  }

  public void declareOutputFields(
      OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("eventid", "location" , "isPaid", "isTech", "st") );
  }

	public void close() 
	{
		try
		{
			writer.flush();
			writer.close();
			System.out.println("Total num of records processed  = " + lineCounter);
			System.out.println("Total num of events = " +eventIds.size());
		}
		catch(Exception e )
		{
			System.out.println("Exception in spout "+ e.getStackTrace());
		}
	}
}

