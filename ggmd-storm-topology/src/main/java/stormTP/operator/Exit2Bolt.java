package stormTP.operator;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import stormTP.stream.StreamEmiter;

public class Exit2Bolt implements IRichBolt {

    private OutputCollector collector;
    private int port = -1;
    private StreamEmiter semit = null;

    public Exit2Bolt(int port) {
        this.port = port;
        this.semit = new StreamEmiter(this.port);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {

        int id = t.getIntegerByField("id");
        int top = t.getIntegerByField("top");
        String nom = t.getStringByField("nom");
        int nbCellsParcourus = t.getIntegerByField("nbCellsParcourus");
        int total = t.getIntegerByField("total");
        int maxcel = t.getIntegerByField("maxcel");

        // Construction du JSON attendu
        String json = String.format(
                "{\"id\":%d,\"top\":%d,\"nom\":\"%s\",\"nbCellsParcourus\":%d,\"total\":%d,\"maxcel\":%d}",
                id, top, nom, nbCellsParcourus, total, maxcel
        );

        // Envoi via StreamEmiter
        this.semit.send(json);

        // On émet également le champ pour compatibilité Storm
        collector.emit(new Values(json));

        collector.ack(t);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
