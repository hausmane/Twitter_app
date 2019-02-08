package kafkaConnector;


import java.io.BufferedReader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.BasicConfigurator;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class TwitterApp {

    private static final Logger logger = LoggerFactory.getLogger(TwitterApp.class);

    private static final String KAFKA_TOPIC = "TWEET";

    private String consumerKey,consumerSecret,accessToken,accessTokenSecret; 
    //key associated with the Twitter app consumer
    //secret associated with the Twitter app consumer
    //access token associated with the Twitter app
    //access token secret
    private String logfile;
    
    private Properties props; 
    private TwitterStream ts;

  
 
     
    //This function read a log file in a Json format.
    //it sends automatically each line i.e. each twitt in a json format 
    //to the KAFKA_TOPIC.
     
    private void readJsonFile(final ProducerConfig conf){
    	final Producer<Integer, String> producer = new Producer<>(conf);
        logger.debug("Reading from file: " + logfile);
        BufferedReader br = null;
        try {
            try {
                br = new BufferedReader(new FileReader(logfile));
                String tweet = null;
                while ((tweet = br.readLine()) != null) {                
                    producer.send(new KeyedMessage<Integer, String>(KAFKA_TOPIC, tweet));
                    logger.debug("\n"+"---------- Keyed messages ---------: " +"\n"+tweet);
                }
                logger.debug("Messages sent");
            } catch (IOException e) {
                logger.error("IO Error while producing messages", e);
                logger.trace(null, e);
            }
        } catch (Exception e) {
            logger.error("Error while producing messages", e);
            logger.trace(null, e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (producer != null) {
                    producer.close();
                }
            } catch (IOException e) {
                logger.trace(null, e);
            }
        }
    }
    
    
    //  This function prepare the configuration regarding the arguments given
    //if mode == 1 the given argument should be : 1 kafkaBroker logfile
    //if mode == 2 two possibilities  : 2 kafkaBroker
    //                                    2 apiKey apiSecret tokenValue tokenSecret kafkaBrokerURL filename
    //Then we put this informations in the right place
    
   
    private void preparConf(String[] args) {
    	int mode = 0; //Initialisation of the mode
		try{	   
    		mode = Integer.parseInt(args[0]); //Get the mode
	    	//Set the properties of the kafka server
	        props = new Properties();
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        props.put("request.required.acks", "1");
	        
	        if (mode == 1) {
	        	props.put("metadata.broker.list", args[1]); 
	        	logfile = args[2];       
	        	props.put("producer.type", "async");
	        }else if (args.length == 6) {
	            consumerKey = args[1];
	            consumerSecret = args[2];
	            accessToken = args[3];
	            accessTokenSecret = args[4];
	            props.put("metadata.broker.list", args[5]);
	        }else if (args.length == 2) {//by default we can use our account
	            consumerKey = "LFyQl5eoTbL4VsAWeChwyjaDH";
	            consumerSecret = "RMXETui6ok0W0GNHwq5D2x8B6uIONc0jvskbjg7mbQGYIFIlBK";
	            accessToken = "4474893503-wvPXypEcXmFFh9GgiMVg0FCwbG9xMwNhnLa2MFw";
	            accessTokenSecret = "CS56xwmRsEdQTk7BmlVYN45lwWApnvBf7QrrQvsIqp0BB";
	            props.put("metadata.broker.list", args[1]);  
	        }
		}catch(Exception e){
			System.out.println("mode is not an integer");
		}
    }

    
    //This function listens tweets coming from the twitter API
    //And put them in the KAFKA_TOPIC
    
   
    private void twitterListener(final ProducerConfig conf) {
        final Producer<String, String> producer = new Producer<>(conf);
        // Configure properties 
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        ts = new TwitterStreamFactory(cb.build()).getInstance();

        //Anonymous class who will be the status listener of tweets
        //We edit the onStatus method such as it we'll be called for each tweet
        StatusListener sl;
        sl = new StatusListener() {
            @Override 
            public void onStatus(Status status) {
                logger.info(status.getUser().getScreenName() + ": " + status.getText());
                KeyedMessage<String, String> data = new KeyedMessage<>(KAFKA_TOPIC, DataObjectFactory.getRawJSON(status));
                producer.send(data);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }

            @Override
            public void onException(Exception ex) {
                logger.info("Shutting down Twitter sample stream...");
                ts.shutdown();
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }
        };

        // Bind the listener 
        ts.addListener(sl);

        ts.sample();
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int mode = 0;
        TwitterApp app = new TwitterApp();
        if (args.length > 0) {
            System.out.println("Started twitter kafka producer with mode: " + args[0]);
            try {
                mode = Integer.parseInt(args[0]);
                if (mode==1) {
                	 if (args.length <3 ) {
                         System.out.println("Mode1 : 1 kafkaBroker logfile");
                         System.exit(1);  
                      }
                }
                if(mode==2){
                	if(args.length!=2 && args.length!=6){
                		System.out.println("Mode2 : 2 apiKey apiSecret tokenValue tokenSecret kafkaBrokerURL");
                		System.out.println("Or Mode2 : 2 kafkaBrokerURL");
                		System.exit(1);
                	}
                }
                app.preparConf(args);
                ProducerConfig config = new ProducerConfig(app.props);
                if (mode == 1) app.readJsonFile(config);
                else app.twitterListener(config);

            } catch (Exception e) {
            	System.out.println("Please read the readme");
                System.exit(1);
            }
            
        } else {
            System.out.println("Please read the readme");
            System.exit(1);
        }

    }

}
