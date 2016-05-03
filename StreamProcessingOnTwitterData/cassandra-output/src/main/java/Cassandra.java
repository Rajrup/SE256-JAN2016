import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class Cassandra {
	private Cluster cluster;
	private Session session;
	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster %s\n",metadata.getClusterName());
		for(Host host:metadata.getAllHosts()){
//			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
//					host.getDatacenter(),
//					host.getAddress(),
//					host.getRack()
//					);
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
			//System.out.print("Creating Keyspace 'twitter'");
		}else{
			//System.out.print("Keyspace 'twitter' exists");
		}
		TableMetadata table = ks.getTable("sentiment_data");
		if(table==null){
			session.execute("CREATE TABLE twitter.sentiment_data ("+
					"country text PRIMARY KEY,"+
					"count double,"+
					"total bigint"+
					");");
			//System.out.print("Creating Table 'sentiment_data'");
		}else{
			//System.out.print("Table 'sentiment_data' exists");
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
	public void printData(String table){
		Statement select= QueryBuilder.select().all().from("twitter",table);
		ResultSet resultSet=session.execute(select);
		for(Row row : resultSet){
			System.out.println(row.getString("country")+"|"+row.getDouble("count")+"|"+row.getLong("total"));
		}
	}
}
