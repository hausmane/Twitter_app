package stormTopology;

import java.io.IOException;
import java.util.Map;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;

public class FirstBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
  //mapper provides functionality for converting between Java objects and matching JSON constructs.
    private static final ObjectMapper mapper = new ObjectMapper();
    String[] lang_list;//The given list of languages

  //We build our FirstBolt with the given list_lang
    public FirstBolt(String[] lang) {
        this.lang_list = lang;
    }

    @SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("Filttering incoming tweets");
        String json = input.getString(0);
        String timestamp_ms,lang;
        String[] hashtags;
        ArrayList<String> hashtagss;
        try {
            JsonNode root = mapper.readValue(json, JsonNode.class);//The root of json tree.
            //We have to make sure the tweet is in a known language.
            if (root.get("lang") != null) {
            	boolean knownLang=false;//boolean saying if the tweet is in a known language
            	lang = root.get("lang").textValue();
            	for (int i = 0; i <= lang_list.length - 1; i++) {
                    if (lang_list[i].equals(lang)) {
                        knownLang=true;  
                    }
                }
                if (knownLang==true && root.get("timestamp_ms") != null && root.get("entities").get("hashtags") != null) {
                   //Once the verifications done we extract the timestamp, lang and the array of hashtags.
                	timestamp_ms = root.get("timestamp_ms").textValue();
                    lang = root.get("lang").textValue();
                    String hs = root.get("entities").get("hashtags").toString();
                    if (hs.length() > 0) {
                        hashtagss = mapper.readValue(root.get("entities").get("hashtags").toString(), ArrayList.class);
                        if (!hashtagss.isEmpty()) {
                        	//For each tweet we emit every tweet with its own timestamp and lang
                            for (int i = 0; i < hashtagss.size(); i++) {
                                String[] hash = hashtagss.toArray()[i].toString().split("=");
                                hashtags = hash[1].split(",");
                                String hashtag=hashtags[0];
                                collector.emit(new Values(timestamp_ms, lang, hashtag));
                                System.out.println("\n"+"hashtag: " + hashtag +"\n"+"lang: "+ lang);
                            }
                        }
                    } 
                } 
            } 
        } catch (IOException ex) {
            System.out.println("IO error :" + ex.getMessage());
        }
    }
    
    //For the next bolt the fields will be the declared ones
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "lang", "hashtag"));
    }

    public Map<String, Object> getComponenetConfiguration() {
        return null;
    }
}
