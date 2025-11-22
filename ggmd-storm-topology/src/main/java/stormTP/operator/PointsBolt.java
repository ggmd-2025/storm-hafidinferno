package stormTP.operator;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PointsBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {
            // Récupération des champs envoyés par GiveRankBolt
            int id = t.getIntegerByField("id");
            int top = t.getIntegerByField("top");
            String rangStr = t.getStringByField("rang"); // ex: "1", "2ex"
            int total = t.getIntegerByField("total");
            int maxcel = t.getIntegerByField("maxcel");

            // Nettoyage du rang pour le calcul (retirer "ex")
            int rang = Integer.parseInt(rangStr.replace("ex", ""));

            // --- FORMULE DE CALCUL DES POINTS ---
            // Exemple : 100 divisé par le rang (Le 1er a 100, le 2eme 50, etc.)
            int points = 0;
            if (rang > 0) {
                points = 100 / rang; 
            }
            // ------------------------------------

            // On émet tout + les points
            collector.emit(t, new Values(id, top, rangStr, total, maxcel, points));
            collector.ack(t);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(t);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // On déclare les champs de sortie incluant "points"
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel", "points"));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}