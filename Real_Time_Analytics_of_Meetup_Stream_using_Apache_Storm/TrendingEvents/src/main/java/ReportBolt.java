import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {

	private static final long serialVersionUID = -4477728832245259434L;
	
	private String outputFilePath;
	
	private File outputFile;
	
	public ReportBolt(String outputFilePath){
		this.outputFilePath = outputFilePath;
	}
	
	public void execute(Tuple tuple) {
		
		try{
			@SuppressWarnings("unchecked")
			LinkedHashMap<String, Integer> tempMap = (LinkedHashMap<String, Integer>) tuple.getValueByField("rankings");
			writeOutput(tempMap);			
		}
	  	catch(Exception e){
	  		System.out.println("Exception in ReportBolt execute()");
	  		e.printStackTrace();
	  	}
	}

	@Override
	public void cleanup() {
		//redis.close();
		super.cleanup();
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		this.outputFile = new File(outputFilePath);
		
		try{
			if(outputFile.exists()){
				outputFile.delete();
				if(!outputFile.createNewFile()){
					throw new Exception("Cannot create output file.");
				}
			}
		}
		catch(Exception e){
			System.out.println("Cannot create output file.");
			e.printStackTrace();
		}
	}
	
	private void writeOutput(LinkedHashMap<String, Integer> tempMap) throws IOException {
		if(tempMap.size() <= 0)
			return;
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile, true));
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone istTimeZone = TimeZone.getTimeZone("Asia/Kolkata");
		sdfDate.setTimeZone(istTimeZone);
		
		Date now = new Date();
		String time = sdfDate.format(now);
		
		System.out.println();
		System.out.println();
		System.out.println(time);
		System.out.println("**********************************************************");
		
		bw.write("\n");
		bw.write("\n");
		bw.write(time);
		bw.write("\n");
		
		bw.write("**********************************************************");
		bw.write("\n");
		
		for(Entry<String, Integer> entry : tempMap.entrySet()){
			String line = entry.getKey() + " - " + entry.getValue();
			System.out.println(line);
			bw.write(line);
			bw.write("\n");				
		}
		
		bw.write("**********************************************************");
		bw.write("\n");
		System.out.println("**********************************************************");
		
		bw.flush();
		bw.close();
	}	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
