import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

public class SlidingWindow {	
	
	private final int size;
		
	private class Window{
		
		private LinkedList<Integer> list;
		
		private int size;
		
		public Window(int size){
			this.list=new LinkedList<Integer>();
			this.size = size;
		}
		
		public int getCountAndAdvance(){
			int sum = 0;			
			
			if(list.size() == 0)
				return sum;
			
			int j = 1;
			for(Integer i : list){
				sum += i;
				
				if(j == this.size){
					System.out.println("breaking j " + j + " size " + this.list.size());
					break;
				}
				else
					j++;
			}
			
			this.list.removeFirst();
			return sum;
		}
		
		public void add(int val) {
			list.add(val);
		}		
	}
	
	private HashMap<String, Window> map;	
	
	public SlidingWindow(int size) throws Exception{
		this.map = new HashMap<String, Window>();
		
		if(size <= 0)
			throw new Exception("Invalid window size");
		else
			this.size = size;
	}
	
	public void put(String key, int val){
		if(map.containsKey(key)){
			map.get(key).add(val);
		}
		else{
			Window window = new Window(this.size);
			window.add(val);
			map.put(key, window);
		}
	}
	
	public HashMap<String, Integer> getAggregateMapAndAdvance(){
		HashMap<String, Integer> aggMap = new HashMap<String, Integer>();
		Iterator<Entry<String, Window>> iter = this.map.entrySet().iterator();
		
		while(iter.hasNext()) {
			Entry<String, Window> entry = iter.next();
			int count = entry.getValue().getCountAndAdvance();
			if(count > 0)
				aggMap.put(entry.getKey(), count);
			else
				iter.remove();
		}
		return aggMap;
	}
}
