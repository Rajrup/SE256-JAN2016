
public class Bucket {

	private int recentTimestamp;
	private int powerOfTwoBucketSize;
	
	public Bucket(int recentTimestamp, int powerOfTwoBucketSize)
	{
		this.recentTimestamp = recentTimestamp;
		this.powerOfTwoBucketSize = powerOfTwoBucketSize;
	}
	
	public int getRecentTimestamp() {
		return recentTimestamp;
	}
	public void setRecentTimestamp(int recentTimestamp) {
		this.recentTimestamp = recentTimestamp;
	}
	public int getPowerOfTwoBucketSize() {
		return powerOfTwoBucketSize;
	}
	public void setPowerOfTwoBucketSize(int powerOfTwoBucketSize) {
		this.powerOfTwoBucketSize = powerOfTwoBucketSize;
	}
}
