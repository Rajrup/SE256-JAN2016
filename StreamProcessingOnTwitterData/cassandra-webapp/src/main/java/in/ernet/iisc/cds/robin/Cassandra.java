package in.ernet.iisc.cds.robin;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Cassandra {
	private Cluster cluster;
	private Session session;
	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster %s\n",metadata.getClusterName());
		for(Host host:metadata.getAllHosts()){
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(),
					host.getAddress(),
					host.getRack()
					);
		}
		session=cluster.connect();
	}
	public void close(){
		cluster.close();
	}
	public void createSchema(){
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace("twitter");
		if(ks==null){
			session.execute("CREATE KEYSPACE twitter WITH replication "+
					"= {'class':'SimpleStrategy','replication_factor':3};");
			ks = cluster.getMetadata().getKeyspace("twitter");
			System.out.println("Creating Keyspace 'twitter'");
		}else{
			System.out.println("Keyspace 'twitter' exists");
		}
		TableMetadata table = ks.getTable("sentiment_data");
		if(table==null){
			session.execute("CREATE TABLE twitter.sentiment_data ("+
					"country text PRIMARY KEY,"+
					"count double,"+
					"total bigint"+
					");");
			System.out.println("Creating Table 'sentiment_data'");
		}else{
			System.out.println("Table 'sentiment_data' exists");
		}
	}
	public void loadData(String country,double count,long total){
		session.execute("INSERT INTO twitter.sentiment_data (country,count,total)"+
				"VALUES ("+
				"'"+country+"',"+
				count+","+
				total+
				");");
	}

	public JSONArray getTotal() {
		Statement select = QueryBuilder.select().all().from("twitter","sentiment_data");
		ResultSet resultSet=session.execute(select);
		JSONArray array=new JSONArray();
		JSONArray object=new JSONArray();
		object.put("Country");
		object.put("Tweets");
		array.put(object);
		for (Row row : resultSet) {
			object=new JSONArray();
			try {
				object.put(row.getString("country"));
				object.put(row.getLong("total"));
			} catch (JSONException e) {
				e.printStackTrace();
			}
			array.put(object);
		}
		return array;
	}

	public class SentimentData{
		public String country;
		public double count;
		public long total;
		public SentimentData(String country, double count, long total) {
			super();
			this.country = country;
			this.count = count;
			this.total = total;
		}
		
	}
	public JSONArray getData(){
		Statement select = QueryBuilder.select().all().from("twitter","sentiment_data");
		ResultSet resultSet=session.execute(select);
		JSONArray array=new JSONArray();
		JSONArray object=new JSONArray();
		object.put("Country");
		object.put("Sentiment");
		array.put(object);
		for (Row row : resultSet) {
			object=new JSONArray();
			try {
				object.put(row.getString("country"));
				object.put(row.getDouble("count"));
				//object.put("total", row.getLong("total"));
			} catch (JSONException e) {
				e.printStackTrace();
			}
			array.put(object);
		}
		return array;
	}
}
