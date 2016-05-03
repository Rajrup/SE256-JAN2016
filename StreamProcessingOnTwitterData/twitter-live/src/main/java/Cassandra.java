import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;


public class Cassandra {
	private static final Logger LOG = Logger.getLogger(Cassandra.class);
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
			LOG.info("Creating Keyspace 'twitter'");
		}else{
			LOG.info("Keyspace 'twitter' exists");
		}
		TableMetadata table = ks.getTable("sentiment_data");
		if(table==null){
			session.execute("CREATE TABLE twitter.sentiment_data ("+
					"country text PRIMARY KEY,"+
					"count double,"+
					"total bigint"+
					");");
			LOG.info("Creating Table 'sentiment_data'");
		}else{
			LOG.info("Table 'sentiment_data' exists");
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
	public void loadData(String country,long total){
		loadData(country,0.0,total);
	}
}
