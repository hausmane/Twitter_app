package stormTopology;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@SuppressWarnings("hiding")
public final class SlotBasedCounterII<String> implements Serializable {

	private static final long serialVersionUID = 1L;

	//We change Hashmap into TreeMap because The map is sorted according to the natural
	//ordering of its keys, or by a Comparator provided at map creation time, depending 
	//on which constructor is used.
	private final Map<String, long[]> hashtagCounter = new TreeMap<String, long[]>();
	private final int numSlots;

	public SlotBasedCounterII(int numSlots) {
		this.numSlots = numSlots;
	}

	public void incrementCount(String hashtag, int slide) {
		long[] counts = hashtagCounter.get(hashtag);
		if (counts == null) {
			counts = new long[this.numSlots];
			hashtagCounter.put(hashtag, counts);
		}
		counts[slide]++;
	}

	public long getCount(String hashtag, int slot) {
		long[] counts = hashtagCounter.get(hashtag);
		if (counts == null) {
			return 0;
		} else {
			return counts[slot];
		}
	}
 
	public Map<String, Long> getCounts() {
		Map<String, Long> result = new TreeMap<String, Long>();
		for (String hashtag : hashtagCounter.keySet()) {
			result.put(hashtag, getTotalCount(hashtag));
		}
		return result;
	}

	public void wipeSlot(int slot) {
		for (String hashtag : hashtagCounter.keySet()) {
			resetSlotCountToZero(hashtag, slot);
		}
	}

	private void resetSlotCountToZero(String hashtag, int slide) {
		long[] counts = hashtagCounter.get(hashtag);
		counts[slide] = 0;
	}

	private boolean shouldBeRemovedFromCounter(String hashtag) {
		return getTotalCount(hashtag) == 0;
	}

	private long getTotalCount(String hashtag) {
		long[] current = hashtagCounter.get(hashtag);
		long total = 0;
		for (long l : current) {
			total += l;
		}
		return total;
	}

  public void wipeZeros() {
    Set<String> hashtagToBeRemoved = new HashSet<String>();
    for (String hashtag : hashtagCounter.keySet()) {
      if (shouldBeRemovedFromCounter(hashtag)) {
    	  hashtagToBeRemoved.add(hashtag);
      }
    }
    for (String hashtag : hashtagToBeRemoved) {
      hashtagCounter.remove(hashtag);
    }
  }

}