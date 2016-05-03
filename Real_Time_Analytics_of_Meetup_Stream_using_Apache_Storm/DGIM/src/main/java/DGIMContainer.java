import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class DGIMContainer {
	
	public Deque<Bucket> bucketSequence;
	private int windowSize;
	private static final int BASE = 2;
	private int maxSameSizeBuckets;
	private int timestamp;
	
	private int latestBits[];
	
	public DGIMContainer(int windowSize, int maxSameSizeBuckets)
	{
		this.timestamp = -1;
		this.bucketSequence = new ArrayDeque<Bucket>();
		
		this.windowSize = windowSize;
		this.maxSameSizeBuckets = maxSameSizeBuckets;
		
		this.latestBits = new int[windowSize];
	}
	
	private Boolean isOldestBucketStale()
	{
		if(bucketSequence.isEmpty())
			return false;
		
		if(bucketSequence.getLast().getRecentTimestamp() == timestamp)
			return true;
		else
			return false;
	}
	
	private void combineBucketsIfRequired()
	{
		int sameBucketSize = 1;
		Bucket current, older;
		Iterator<Bucket> bucketIterator = bucketSequence.iterator();
		Boolean mergeHappened = false;

		if (bucketIterator.hasNext())
		{
			current = bucketIterator.next();
			
			while(bucketIterator.hasNext())
			{
				older = bucketIterator.next();
				//System.out.println("Timestamp: " + older.getRecentTimestamp() + " BucketSize: " + older.getPowerOfTwoBucketSize() + " SameBucketSize: " + sameBucketSize);
				if(current.getPowerOfTwoBucketSize() != older.getPowerOfTwoBucketSize())
				{
					sameBucketSize = 1;
				}
				else
				{
					sameBucketSize++;
					
					if(sameBucketSize > maxSameSizeBuckets)
					{
						mergeHappened = true;
						//System.out.println("Merging " + older.getRecentTimestamp() + " and " + current.getRecentTimestamp());
						older.setRecentTimestamp(current.getRecentTimestamp());
						older.setPowerOfTwoBucketSize(older.getPowerOfTwoBucketSize() + 1);
						
						bucketSequence.remove(current);
						
						sameBucketSize = 1;
					}
				}
				current = older;
			}
		}
	}
	
	public void consumeInputBit(int bit)
	{
		timestamp = (timestamp + 1) % windowSize;
		latestBits[timestamp] = bit;
		
		if(isOldestBucketStale())
		{
			Bucket removedBucket = bucketSequence.removeLast();
			//System.out.println("Removed: " + removedBucket.getRecentTimestamp() + " : " + removedBucket.getPowerOfTwoBucketSize());
		}
		
		if(bit == 1)
		{
			Bucket currentBucket = new Bucket(timestamp, 0);
			bucketSequence.addFirst(currentBucket);
			
			combineBucketsIfRequired();
		}
	}
	
	private int getPositiveModulo(int number, int modulo)
	{
		int reminder = number % modulo;
		if (reminder < 0)
		{
			reminder = reminder + modulo;
		}
		return reminder;
	}
	
	public int getCount(int intervalSize)
	{
		Iterator<Bucket> bucketIterator = bucketSequence.descendingIterator();
		int count = 0;
		Boolean oldestBucketIntersectingIntervalFound = false;
		
		while(bucketIterator.hasNext())
		{
			Bucket currentBucket = bucketIterator.next();
			if(getPositiveModulo(this.timestamp - currentBucket.getRecentTimestamp(), this.windowSize) < intervalSize)
			{
				if(oldestBucketIntersectingIntervalFound == false)
				{
					oldestBucketIntersectingIntervalFound = true;
					count = count + (int) Math.ceil(Math.pow(BASE,currentBucket.getPowerOfTwoBucketSize())/2);
					/* System.out.println("OldestBucketIntersectingIntervalFound. Timestamp: " + currentBucket.getRecentTimestamp()
							+ " BucketSize: " + currentBucket.getPowerOfTwoBucketSize()); */
				}
				else
				{
					count = count + (int) Math.pow(BASE,currentBucket.getPowerOfTwoBucketSize());
				}
			}
			 
		}
		
		return count;
	}

	public int getAccurateCount(int intervalSize)
	{
		int sum = 0;
		for (int i = 0 ; i < intervalSize ; i++)
		{
			sum += latestBits[getPositiveModulo(timestamp - i,  windowSize)]; 
		}
		return sum;
	}
	
	/* public static void main(String[] args)
	{
		DGIMContainer dgimContainer = new DGIMContainer(8,2);
		dgimContainer.bucketSequence.add(new Bucket(1,0));
		dgimContainer.bucketSequence.add(new Bucket(2,0));
		dgimContainer.bucketSequence.add(new Bucket(3,1));
		dgimContainer.bucketSequence.add(new Bucket(4,1));
		dgimContainer.bucketSequence.add(new Bucket(4,2));
		
		Iterator<Bucket> bucketIterator = dgimContainer.bucketSequence.iterator();
		while(bucketIterator.hasNext())
		{
			Bucket next = bucketIterator.next();
			next.setRecentTimestamp(next.getRecentTimestamp()+10);
			next.setPowerOfTwoBucketSize(next.getPowerOfTwoBucketSize()+10);
		}
		
		System.out.println("Before removal:");
		bucketIterator = dgimContainer.bucketSequence.iterator();
		Bucket next = new Bucket(0,0);
		Bucket third = new Bucket(0,0);
		int i = 0;
		while(bucketIterator.hasNext())
		{
			i++;
			next = bucketIterator.next();
			if(i == 3)
			{
				third = next;
			}
			System.out.println(next.getRecentTimestamp() + ": " + next.getPowerOfTwoBucketSize());
		}
		
		dgimContainer.bucketSequence.remove(third);
		
		System.out.println("\nAfter removal:");
		bucketIterator = dgimContainer.bucketSequence.iterator();
		while(bucketIterator.hasNext())
		{
			Bucket next1 = bucketIterator.next();
			System.out.println(next1.getRecentTimestamp() + ": " + next1.getPowerOfTwoBucketSize());
		}
	} */

}
