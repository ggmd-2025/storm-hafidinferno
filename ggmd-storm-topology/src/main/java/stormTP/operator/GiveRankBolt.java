package stormTP.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class GiveRankBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String raw = input.getStringByField("json");

            // Extraction du bloc "runners" ou "tortoises"
            int idx = raw.indexOf("\"runners\":[");
            if (idx < 0) idx = raw.indexOf("\"tortoises\":[");
            
            if (idx < 0) {
                collector.ack(input);
                return;
            }

            int start = idx + raw.substring(idx).indexOf("[");
            int end = raw.lastIndexOf("]"); 
            String runnersBlock = raw.substring(start + 1, end);

            // Découpage manuel des objets JSON
            List<String> tortuesJSON = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            int brace = 0;
            for (char c : runnersBlock.toCharArray()) {
                if (c == '{') brace++;
                if (brace > 0) current.append(c);
                if (c == '}') {
                    brace--;
                    if (brace == 0) {
                        tortuesJSON.add(current.toString());
                        current = new StringBuilder();
                    }
                }
            }

            // Classe interne temporaire
            class Runner {
                int id, top, total, maxcel, nbCells;
                String nom; // IMPORTANT : Ajout du champ nom
            }

            List<Runner> list = new ArrayList<>();

            for (String t : tortuesJSON) {
                Runner r = new Runner();
                r.id = extractInt(t, "\"id\":");
                r.top = extractInt(t, "\"top\":");
                r.total = extractInt(t, "\"total\":");
                r.maxcel = extractInt(t, "\"maxcel\":");
                
                // IMPORTANT : Extraction du nom
                r.nom = extractString(t, "\"nom\":"); 

                int tour = extractInt(t, "\"tour\":");
                int cellule = extractInt(t, "\"cellule\":");
                r.nbCells = tour * r.maxcel + cellule;

                list.add(r);
            }

            // Tri décroissant
            list.sort((a, b) -> Integer.compare(b.nbCells, a.nbCells));

            // Calcul et émission
            int currentRank = 1;
            for (int i = 0; i < list.size(); i++) {
                Runner r = list.get(i);
                String rang;

                if (i > 0 && list.get(i).nbCells == list.get(i - 1).nbCells) {
                    rang = currentRank + "ex";
                } else {
                    if (i > 0) currentRank = i + 1;
                    rang = String.valueOf(currentRank);
                }

                // IMPORTANT : On émet "nom" ici
                collector.emit(new Values(r.id, r.top, r.nom, rang, r.total, r.maxcel));
            }

            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    private int extractInt(String json, String key) {
        int i = json.indexOf(key);
        if (i < 0) return 0;
        i += key.length();
        int j = i;
        while (j < json.length() && (Character.isDigit(json.charAt(j)) || json.charAt(j) == '-')) j++;
        try { return Integer.parseInt(json.substring(i, j)); } catch(Exception e) { return 0; }
    }

    private String extractString(String json, String key) {
        int i = json.indexOf(key);
        if (i < 0) return "";
        i += key.length();
        if (i < json.length() && json.charAt(i) == '"') i++;
        int j = json.indexOf('"', i);
        if (j < 0) return "";
        return json.substring(i, j);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // IMPORTANT : On déclare le champ "nom" ici
        declarer.declare(new Fields("id", "top", "nom", "rang", "total", "maxcel"));
    }

    @Override
    public void cleanup() {}
    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}