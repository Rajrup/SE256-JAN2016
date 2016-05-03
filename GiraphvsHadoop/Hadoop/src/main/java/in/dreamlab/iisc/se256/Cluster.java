package in.dreamlab.iisc.se256;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class Cluster{
	String ClusterId;
	long InternalWeightCount;
	long BoundaryWeightCount;
	double score;
	ArrayList<String> VertexList;
	
	
	public Cluster(){
		
		VertexList=new ArrayList<String>();
		BoundaryWeightCount=0;
		InternalWeightCount=0;
		score=1;
	}
	
	
	public Cluster(String clusterString){
		String[] data=clusterString.split(":");
		ClusterId=data[0];
		InternalWeightCount=Long.parseLong(data[1]);
		BoundaryWeightCount=Long.parseLong(data[2]);
		score=Double.parseDouble(data[3]);
		VertexList=new ArrayList<String>();
		
		for(int i=4;i<data.length;i++){
			VertexList.add(data[i]);
		}
		
	}
	
	
	
	
	public boolean Contains(String VertexId){
		
		if(VertexList.contains(VertexId)){
			return true;
		}
		
		return false;
	}
	
	public long Count()
	{
		return VertexList.size();
	}
	
	public void AddVertex(String VertexId,String AdjacencyList){
		
		VertexList.add(VertexId);
		
		String[] Neighbours = AdjacencyList.split(",");
		
		for(String Neighbour: Neighbours){
			if(this.Contains(VertexId)){
				BoundaryWeightCount--;
				InternalWeightCount++;
			}
			else
				BoundaryWeightCount++;
		}
		
		if(VertexList.size()>1){
			CalculateScore();
		}
		
	}
	
	void CalculateScore(){
		long count=VertexList.size();
		score=(double)(InternalWeightCount-0.5*BoundaryWeightCount)/(count*(count-1));
	}
//
//	public void readFields(DataInput arg0) throws IOException {
//		// TODO Auto-generated method stub
//		 ClusterId=arg0.readUTF();
//		 InternalWeightCount=arg0.readLong();
//		 BoundaryWeightCount=arg0.readLong();
//		 score=arg0.readDouble();
//		 VertexList=arg0.
//	}
//
//	public void write(DataOutput  out) throws IOException {
//		// TODO Auto-generated method stub
//		out.writeUTF(ClusterId);
//		out.writeLong(InternalWeightCount);
//		out.writeLong(BoundaryWeightCount);
//		out.writeDouble(score);
//		
//	}
//
//	public int compareTo(Object o) {
//		// TODO Auto-generated method stub
//		double thisValue=this.score;
//		Cluster c=(Cluster)o;
//		double thatValue=c.score;
//		
//		return  (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
//	}


	public String toString(){
	String clusterString="";	
	clusterString=ClusterId+":"+InternalWeightCount + ":" +BoundaryWeightCount+":"+score;
	
	for(String v:VertexList){
		clusterString+=":" +v;
	}
	
	return clusterString;
	}
	
	
	
}