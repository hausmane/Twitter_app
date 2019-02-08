package stormTopology;


import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private ConsumerConnector consumer; //Interface of the consumer
    private SpoutOutputCollector collector;//spout that can tag messages with ids so that they can be acked or failed later on
    String zk_url,id;//zookeeper url and group id for the properties of "consumer"
    private static final String KAFKA_TOPIC = "TWEET";

    //We build our KafkaSpout with the given zookeeper url and the group id
    public KafkaSpout(String zk_url, String id) {
        this.zk_url = zk_url;
        this.id = id;
    }

    //We declare the next output field for the topology.
    //for the next bolt
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("Tweet"));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }
    
    //This function prepare the configuration for the consumer
    private kafka.consumer.ConsumerConfig createConsumerConfig(String zk_url, String id) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zk_url);//we give the zookeeper url
        props.put("group.id", id);//and the group id
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    @SuppressWarnings("rawtypes")
    //Called when a task for this component is initialized within a worker on the cluster.
    //It provides the spout with the environment in which the spout executes.
    //Fill the collector and the consumer's interface with all the parameters
	public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zk_url, id));
        this.collector = soc;
    }

    //When this method is called, Storm is requesting that the Spout emit tuples to the output collector. 
    public void nextTuple() {
        Map<String, Integer> topic = new HashMap<String, Integer>();
        topic.put(KAFKA_TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> cons = consumer.createMessageStreams(topic);
        List<KafkaStream<byte[], byte[]>> tweetList = cons.get(KAFKA_TOPIC);
        ConsumerIterator<byte[], byte[]> ci = tweetList.get(0).iterator();
        while (ci.hasNext()) collector.emit( new Values(new String(ci.next().message())));
    }

}
