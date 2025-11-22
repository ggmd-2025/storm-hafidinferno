package stormTP.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MyTortoiseBolt implements IRichBolt {

    private OutputCollector collector;
    private int idFiltre;
    private String nom;

    public MyTortoiseBolt(int idFiltre, String nom) {
        this.idFiltre = idFiltre;
        this.nom = nom;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        try {
            String raw = input.getStringByField("json");

            // On extrait juste la section runners : [...]
            int i1 = raw.indexOf("\"runners\":");
            if (i1 == -1) {
                collector.ack(input);
                return;
            }

            int start = raw.indexOf('[', i1);
            int end = raw.indexOf(']', start);

            if (start == -1 || end == -1) {
                collector.ack(input);
                return;
            }

            String runnersBlock = raw.substring(start + 1, end); // contenu entre [ et ]

            // Split des objets tortue ; chaque tortue est entre { ... }
            String[] tortues = runnersBlock.split("\\},\\{");

            for (String t : tortues) {

                // nettoyage des accolades
                t = t.replace("{", "").replace("}", "");

                // chaque champ : cle:valeur
                String[] champs = t.split(",");

                int id = -1, top = 0, tour = 0, cellule = 0, total = 0, maxcel = 0;

                for (String c : champs) {
                    String[] kv = c.split(":");

                    if (kv.length < 2) continue;

                    String key = kv[0].replace("\"", "").trim();
                    String val = kv[1].replace("\"", "").trim();

                    switch (key) {
                        case "id": id = Integer.parseInt(val); break;
                        case "top": top = Integer.parseInt(val); break;
                        case "tour": tour = Integer.parseInt(val); break;
                        case "cellule": cellule = Integer.parseInt(val); break;
                        case "total": total = Integer.parseInt(val); break;
                        case "maxcel": maxcel = Integer.parseInt(val); break;
                    }
                }

                // Si c'est la tortue filtrée
                if (id == idFiltre) {
                    int nbCellsParcourus = tour * maxcel + cellule;

                    collector.emit(new Values(
                            id, top, nom, nbCellsParcourus, total, maxcel
                    ));
                }
            }

            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "id","top","nom","nbCellsParcourus","total","maxcel"
        ));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String,Object> getComponentConfiguration(){ return null; }
}
