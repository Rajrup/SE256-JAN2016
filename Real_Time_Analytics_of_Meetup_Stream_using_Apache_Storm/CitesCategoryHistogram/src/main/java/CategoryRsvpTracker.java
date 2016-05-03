import java.util.HashMap;
import java.util.Map;

public class CategoryRsvpTracker
{
	int totalRsvpCount;
	Map<String, Integer> categoryCount;
	
	public CategoryRsvpTracker()
	{
		totalRsvpCount = 0;
		categoryCount = new HashMap<String, Integer>();
	}
	
	public CategoryRsvpTracker(int count, Map <String, Integer> categoryCounterMap)
	{
		totalRsvpCount = count;
		categoryCount = categoryCounterMap;
	}
}
