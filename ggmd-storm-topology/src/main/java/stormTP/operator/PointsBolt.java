package stormTP.operator;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import stormTP.core.Manager;

public class PointsBolt implements IRichBolt {
    private OutputCollector collector;
    private Manager manager;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.manager = new Manager();
    }

    @Override
    public void execute(Tuple input) {
        try {
            String jsonInput = input.getStringByField("json");
            
            // Appel au Manager pour calculer les points
            String jsonOutput = manager.computePoints(jsonInput);
            
            collector.emit(input, new Values(jsonOutput));
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}