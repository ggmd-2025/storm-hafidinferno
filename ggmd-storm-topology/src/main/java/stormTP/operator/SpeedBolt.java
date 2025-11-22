package stormTP.operator;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpeedBolt extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        
        // On a besoin d'au moins 2 tuples pour calculer une vitesse (début et fin)
        if (tuples.size() < 2) return;

        // 1. Extraire l'état des coureurs au DÉBUT de la fenêtre
        Tuple startTuple = tuples.get(0);
        Map<Integer, RunnerData> startData = parseRunners(startTuple.getStringByField("json"));

        // 2. Extraire l'état des coureurs à la FIN de la fenêtre
        Tuple endTuple = tuples.get(tuples.size() - 1);
        Map<Integer, RunnerData> endData = parseRunners(endTuple.getStringByField("json"));

        // 3. Calculer la vitesse pour chaque coureur identifié
        for (Integer id : startData.keySet()) {
            if (endData.containsKey(id)) {
                RunnerData start = startData.get(id);
                RunnerData end = endData.get(id);

                // Calcul de la distance parcourue
                // Distance totale = (Nombre de tours * Taille piste) + Cellule actuelle
                long distStart = (long) start.tour * start.maxcel + start.cellule;
                long distEnd = (long) end.tour * end.maxcel + end.cellule;
                long distance = distEnd - distStart;

                // Calcul du temps écoulé (en tops)
                long timeDelta = end.top - start.top;

                double vitesse = 0.0;
                if (timeDelta > 0) {
                    vitesse = (double) distance / timeDelta;
                }

                // Format de la chaîne "tops": "ti-ti+9"
                String tops = start.top + "-" + end.top;

                // Emission: (id, nom, tops, vitesse)
                collector.emit(new Values(id, start.nom, tops, vitesse));
            }
        }
    }

    // Méthode utilitaire pour parser le JSON complet et extraire les données utiles
    private Map<Integer, RunnerData> parseRunners(String jsonStr) {
        Map<Integer, RunnerData> result = new HashMap<>();
        try {
            JsonReader reader = Json.createReader(new StringReader(jsonStr));
            JsonObject root = reader.readObject();
            
            String key = root.containsKey("tortoises") ? "tortoises" : "runners";
            if (root.containsKey(key)) {
                JsonArray runners = root.getJsonArray(key);
                for (JsonValue v : runners) {
                    JsonObject r = (JsonObject) v;
                    RunnerData data = new RunnerData();
                    data.id = r.getInt("id");
                    data.top = r.containsKey("top") ? r.getInt("top") : 0;
                    data.tour = r.containsKey("tour") ? r.getInt("tour") : 0;
                    data.cellule = r.containsKey("cellule") ? r.getInt("cellule") : 0;
                    data.maxcel = r.containsKey("maxcel") ? r.getInt("maxcel") : 150;
                    data.nom = r.containsKey("nom") ? r.getString("nom") : "Runner" + data.id;
                    
                    result.put(data.id, data);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    // Classe interne simple pour stocker les infos temporairement
    class RunnerData {
        int id;
        long top;
        int tour;
        int cellule;
        int maxcel;
        String nom;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "nom", "tops", "vitesse"));
    }
}