package stormTopology;

import backtype.storm.task.OutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ThirdBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    public ThirdBolt() throws IOException {
    }

    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
     this.collector = oc;
    }
    
    @SuppressWarnings("unchecked")
	public void execute(Tuple tuple) {
        List<Entry<String, Long>> list;
    	if(tuple.size()>2){
         list= (List<Entry<String, Long>>) tuple.getValue(0);  	
    	 Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
             public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                 return (o2.getValue()).compareTo(o1.getValue());
             }
         });
    	 Integer a=Integer.valueOf(tuple.getString(1));
    	 int lowerBound = a;
    	 String lang=tuple.getValue(2).toString();
         //From here we put the 3 firsts  hashtags in a table, and the associated count in an other table
     	int i = 0;
         String []topht=new String[3];
         Long []topcount=new Long[3];
         for (Map.Entry<String, Long> entry : list) {
             topht[i]=entry.getKey();
             topcount[i]=entry.getValue();
             i++;
             if (i == 3) {
             	long real_time_milisec=(long)lowerBound*1000;
                 collector.emit(new Values(lang,real_time_milisec,topht[0],topcount[0],topht[1],topcount[1],topht[2],topcount[2]));
                 break;
             }         
         }

    	}
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    	ofd.declare(new Fields("lang","currentLowerLimit", "hashtag1", "count1","hashtag2", "count2","hashtag3", "count3"));
    }

}