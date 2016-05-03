import java.util.HashMap;
import java.util.Map;

public class CategoryEventTracker
{
	int totalEventCount;
	Map<String, Integer> categoryCount;
	
	public CategoryEventTracker()
	{
		totalEventCount = 0;
		categoryCount = new HashMap<String, Integer>();
	}
	
	public CategoryEventTracker(int count, Map <String, Integer> categoryCounterMap)
	{
		totalEventCount = count;
		categoryCount = categoryCounterMap;
	}
}
