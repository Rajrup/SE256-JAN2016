package in.dreamlab.iisc.trans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
///import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
//import org.archive.io.ArchiveReader;
//import org.archive.io.ArchiveRecord;
//import org.jsoup.Jsoup;
//import org.jsoup.helper.Validate;
//import org.jsoup.nodes.Document;
//import org.jsoup.nodes.Element;
//import org.jsoup.select.Elements;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//import in.ac.iisc.cds.se256.alpha.cc.pojo.IntTextPairWritable;

import java.net.URL;

public class CCAnalyticsWebCrawlerMap
{
	private static final Logger LOG = Logger.getLogger(CCAnalyticsWebCrawlerMap.class);
	
	protected static enum MAPPERCOUNTER 
	{
		URL_FOUND,
		RECORDS_OUT,
		HTML_IN,
		EXCEPTIONS
	}
	
	protected static class CCAnalyticsWebCrawlerMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		//private Text outKeyOwnerURL = new Text();
		////private IntTextPairWritable outValReferredURL = new IntTextPairWritable();
		///private HashMap<String,Boolean> uniqueLinks = new HashMap<String,Boolean>();
		//private HashMap mappingHm=new HashMap();
		//private HashMap hstop=new HashMap();
		//private HashMap hsearch=new HashMap();
		//private String words[];
		//private HashMap reverselookup=new HashMap();
		private HashMap hreachable=new HashMap();


		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//int dimension=3;
			/*BufferedReader in = new BufferedReader(new FileReader("hdfs://sslcluster:9000/user/secprav/mapping/mapping.txt"));
			String str;

			List<String> list = new ArrayList<String>();
			while((str = in.readLine()) != null){
				list.add(str);
			}*/
			//org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
			String j=conf.get("i");
			if(j.equals("0")){
				String vertex=conf.get("vertex");
				hreachable.put(vertex,0);
			}
			else {
				String file_name = conf.get("file_name");


				//Path pat=new Path("hdfs://localhost:9000/parameter/parameters.txt");
			/*Path pat=new Path("hdfs://localhost:9000/"+file_name+"/part-r-*");


			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pat)));
			String line;
			List<String> list = new ArrayList<String>();

			line=br.readLine();
			while (line != null){
				list.add(line);
				line=br.readLine();
			}*/
				FileSystem fs = FileSystem.get(new Configuration());
				List<String> list = new ArrayList<String>();
				FileStatus[] status = fs.listStatus(new Path("hdfs://sslcluster:9000/" + file_name));
				//FileStatus[] status = fs.listStatus(new Path("hdfs://localhost:9000/" + file_name));
				//FileStatus[] status = fs.listStatus(new Path("hdfs://orion-00:9000/" + file_name));
				for (int i = 0; i < status.length; i++) {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String line;
					line = br.readLine();
					while (line != null) {
						//System.out.println(line);
						list.add(line);
						line = br.readLine();
					}
				}

				String[] parameters = list.toArray(new String[0]);


				for (int i = 0; i < parameters.length; i++) {
					if (!hreachable.containsKey(parameters[i].trim())) {
						hreachable.put(parameters[i].trim(), 0);
					}
				/*String[] arr=mapping[i].split("\\t+");
				url=arr[1].trim();
				id=arr[0].trim();
				mappingHm.put(url, Integer.parseInt(id));*/


				}





				/*String[] arr=mapping[i].split("\\t+");
				url=arr[1].trim();
				id=arr[0].trim();
				mappingHm.put(url, Integer.parseInt(id));*/
			}

			}



		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException
		{
		try{
			String[] vertexPair = value.toString().split("\\s+");
			//if(vertexPair.length != 2) throw new Exception("Vertex input format not correct: '"+value+"' did not have 2 tokens:"+vertexPair.length);


			if(vertexPair[0].charAt(0)!='#'&&vertexPair[0].charAt(0)!='p'&&vertexPair[0].charAt(0)!='c'){
				String source = vertexPair[0].trim();
				String sink = vertexPair[1].trim();
			if(hreachable.containsKey(source)){
				context.write(new Text(sink),new Text(""));
				context.write(new Text(source),new Text(""));
			}}








			}






		 catch (Exception e){
			System.out.println("caught exception"+e);
		}













		}
	}
}
		//int count=0;


