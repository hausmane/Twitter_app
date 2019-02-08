package stormTopology;

 //Changer les guillemets dans la topology
// Changer les noms des variables
// Changer le premier if

import backtype.storm.Config;


import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 * 
 */
public class Top3App
{   
    public static void main(String[] args) throws Exception
    {
        String[] langs;
        int DEFAULT_ADVANCE_WINDOW_IN_SECONDS, DEFAULT_SLIDING_WINDOW_IN_SECONDS;
        String topology,folder,zookeper_url;
        BasicConfigurator.configure();
        
        if (args != null){     
        	try{
	        	langs=args[0].split(",");
	        	zookeper_url=args[1];   
	        	String[] window_params=args[2].split(",");
	        	DEFAULT_SLIDING_WINDOW_IN_SECONDS=Integer.parseInt(window_params[0]);
	        	DEFAULT_ADVANCE_WINDOW_IN_SECONDS=Integer.parseInt(window_params[1]);
	        	topology=args[3];
	        	folder=args[4];
	        	LocalCluster cluster = new LocalCluster();
	        	int workers = 4;
	            Config conf = new Config();
	            conf.setDebug(true);
	            conf.setNumWorkers(workers);
	        	cluster.submitTopology(topology,conf,createTopology(langs,zookeper_url,DEFAULT_SLIDING_WINDOW_IN_SECONDS,DEFAULT_ADVANCE_WINDOW_IN_SECONDS,folder));
	            TopologyBuilder tb = new TopologyBuilder();
	            tb.setSpout("kafkaSpout", new KafkaSpout(zookeper_url,"kafkaSpout"), 2);
	            tb.setBolt("FirstBolt", new FirstBolt(langs), 2)
	                     .fieldsGrouping("kafkaSpout", new Fields("Tweet"));   
	            tb.setBolt("SecondBolt", new SecondBolt(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_ADVANCE_WINDOW_IN_SECONDS))
	                    .fieldsGrouping("FirstBolt", new Fields("hashtag"));
	            tb.setBolt("ThirdBolt", new ThirdBolt())
	                 .fieldsGrouping("SecondBolt", new Fields("lang"));
	            tb.setBolt("output-result", new FourthBolt(folder))
	                    .fieldsGrouping("ThirdBolt", new Fields("lang"));
	      
	        	Thread.sleep(90000);
	        	cluster.shutdown();
        	}catch(Exception e){
        		System.out.println("Please read the readme2");
                System.exit(1);
        	}
        }
        else{
        	System.out.println("Please read the readme");
            System.exit(1);
        }
    }

    private static StormTopology createTopology(String[] langs,String zookeper_url, int window_Size, int window_Advance, String folder) throws IOException {
        TopologyBuilder tb = new TopologyBuilder();
       tb.setSpout("kafkaSpout", new KafkaSpout(zookeper_url,"kafkaSpout"), 2);
       tb.setBolt("FirstBolt", new FirstBolt(langs), 2)
                .fieldsGrouping("kafkaSpout", new Fields("Tweet"));   
       tb.setBolt("SecondBolt", new SecondBolt(window_Size, window_Advance))
               .fieldsGrouping("FirstBolt", new Fields("hashtag"));
       tb.setBolt("ThirdBolt", new ThirdBolt())
            .fieldsGrouping("SecondBolt", new Fields("lang"));
       tb.setBolt("output-result", new FourthBolt(folder))
               .fieldsGrouping("ThirdBolt", new Fields("lang"));
       return tb.createTopology();
    }   
}