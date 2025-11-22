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
            System.out.println("RAW >>> " + raw);

            // ---------- EXTRAIRE runners ---------- //
            int idx = raw.indexOf("\"runners\":[");
            if (idx < 0) {
                collector.ack(input);
                return;
            }

            int start = idx + 11;
            int end = raw.lastIndexOf("]");
            String runnersBlock = raw.substring(start, end);

            // ---------- EXTRAIRE CHAQUE TORTUE ---------- //
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

            // ---------- STRUCTURE POUR TRIER ---------- //
            class Runner {
                int id, top, total, maxcel, nbCells;
            }

            List<Runner> list = new ArrayList<>();

            for (String t : tortuesJSON) {
                Runner r = new Runner();
                r.id = extractInt(t, "\"id\":");
                r.top = extractInt(t, "\"top\":");
                r.total = extractInt(t, "\"total\":");
                r.maxcel = extractInt(t, "\"maxcel\":");

                int tour = extractInt(t, "\"tour\":");
                int cellule = extractInt(t, "\"cellule\":");

                r.nbCells = tour * r.maxcel + cellule;

                list.add(r);
            }

            // ---------- TRIER DECROISSANT ---------- //
            list.sort((a, b) -> Integer.compare(b.nbCells, a.nbCells));

            // ---------- CALCULER LES RANGS (CORRECT) ---------- //
            int currentRank = 1;

            for (int i = 0; i < list.size(); i++) {

                Runner r = list.get(i);
                String rang;

                // EX-AEQUO
                if (i > 0 && list.get(i).nbCells == list.get(i - 1).nbCells) {
                    rang = currentRank + "ex";
                }
                else {
                    if (i > 0)
                        currentRank = i + 1;   // ✅ CORRECTION IMPORTANTE

                    rang = String.valueOf(currentRank);
                }

                // LOG
                System.out.println("EMIT RANK >>> id=" + r.id + " rang=" + rang + " nbCells=" + r.nbCells);

                // EMISSION VERS Exit3Bolt
                collector.emit(new Values(r.id, r.top, rang, r.total, r.maxcel));
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
        while (j < json.length() && Character.isDigit(json.charAt(j)))
            j++;

        return Integer.parseInt(json.substring(i, j));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel"));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
