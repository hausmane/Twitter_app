package stormTopology;

import java.io.Serializable;
import java.util.Map;


@SuppressWarnings("hiding")
public final class SlidingWindowCounterII<String> implements Serializable {

  private static final long serialVersionUID = 1L;
  private SlotBasedCounterII<String> hashtagCounter;
  private int headSlot;
  private int tailSlot;
  private int windowLengthInSlots;
  public int slidingCount;

  public SlidingWindowCounterII(int windowLengthInSlots, int slidingCount) {
    this.windowLengthInSlots = windowLengthInSlots;
    this.slidingCount=slidingCount;
    this.hashtagCounter = new SlotBasedCounterII<String>(this.windowLengthInSlots);
    this.headSlot = 0;
    this.tailSlot = slideAfter(headSlot);
  }

  public void incrementCount(String hashtag) {
    hashtagCounter.incrementCount(hashtag, headSlot);
  }

  private void advanceHead() {
    headSlot = tailSlot;
    tailSlot = slideAfter(tailSlot);
  }

  private int slideAfter(int slide) {
    return (slide + 1) % windowLengthInSlots;
  }
  
  public Map<String, Long> getCountsThenAdvanceWindow(int n_intervals_advance) {
	    Map<String, Long> counts = hashtagCounter.getCounts();
	    hashtagCounter.wipeZeros();
	    hashtagCounter.wipeSlot(tailSlot);
	    advanceHead();
	    slidingCount = slidingCount + n_intervals_advance;
	    return counts;
  }


}
