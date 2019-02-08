package stormTopology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class FourthBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final String ID = "09";
    String path;

    public FourthBolt(String path) throws IOException {
        this.path = path;
    }

    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {}

    public void execute(Tuple tuple) {
        if (tuple.size() >= 7) {
        	//String [] table = new String[7]
        	//for (int i = 0; i < tuple.size(); i++) {
				//table[i]=tuple.getValue(i).toString();
			//}
        	//String line = table[0] + "," + table[1] + "," + table[2] + "," + table[3] ...;
        	String lang = tuple.getValue(0).toString();
        	String lowerbound = tuple.getValue(1).toString();
        	String h1 = tuple.getValue(2).toString();
        	String c1 = tuple.getValue(3).toString();
        	String h2 = tuple.getValue(4).toString();
        	String c2 = tuple.getValue(5).toString();
        	String h3 = tuple.getValue(6).toString();
        	String c3 = tuple.getValue(7).toString();
            File file = new File(path + "/" + lang + "_" + ID + ".log");
            String line = lowerbound + "," + h1 + "," + c1 + "," + h2 + "," + c2 + "," + h3 + "," + c3;
            BufferedWriter bw = null;
            try {
                bw = new BufferedWriter(new FileWriter(file, true));
                bw.append(line);
                bw.newLine();
                bw.close();
            } catch (FileNotFoundException ex) {
                System.out.println(ex.getMessage());
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {}

}
