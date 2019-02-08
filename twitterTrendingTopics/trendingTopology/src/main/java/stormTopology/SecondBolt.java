package stormTopology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

 

public class SecondBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private final Map<String,SlidingWindowCounterII<String>>  mapofcounts;
	private OutputCollector collector;
	private final int sliding_window,step;
	
  
	public SecondBolt(int sliding_window, int step) {
		this.sliding_window = sliding_window;
		this.step = step;
		this.mapofcounts = new HashMap<String,SlidingWindowCounterII<String>>();
	}
  
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

     
	public void execute(Tuple tuple) {
		if (tuple.size() >= 3) {
			//1st we get the lang and the ts
			String lang = (String) tuple.getValue(1);
			long timestamp = Long.valueOf(tuple.getString(0))/1000;//We convert into second
			System.out.println("\n"+"---- TIMESTAMP ---- "+"\n"+": "+ timestamp);
			SlidingWindowCounterII<String> ws;
			//We create a sliding window ws
			int slid=this.sliding_window/this.step;
			int inter=(int) timestamp/this.step;
			if ( mapofcounts.get(lang) == null) {
				ws = new SlidingWindowCounterII<String>(slid,inter);
				mapofcounts.put(lang, ws);
			}//Here we create the window of count hashtags for a given language.
			int lowerBound=(mapofcounts.get(lang).slidingCount)*this.step;
			if (timestamp-lowerBound<step) { 
				int inters =(int) (timestamp-lowerBound)/(this.step);
				Map<String, Long> counts = mapofcounts.get(lang).getCountsThenAdvanceWindow(inters);
				emit(counts,lang);
			} //We increment the map of counts for the good hashtag
			String hashtag = tuple.getValue(2).toString();
			mapofcounts.get(tuple.getValue(1).toString()).incrementCount(hashtag);
		}
	}
	
	private void emit(Map<String, Long> hashtag_counts, String lang) {
		//We compute the lower bound for the next bolt operation
		int lowerBound=(mapofcounts.get(lang).slidingCount)*this.step;
		Set<Entry<String, Long>> set = hashtag_counts.entrySet();
		List<Entry<String, Long>> liste = new ArrayList<Entry<String, Long>>(set);
		collector.emit(new Values(liste, lowerBound,lang));
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag_counts","currentLowerLimit", "lang"));
	}
}
