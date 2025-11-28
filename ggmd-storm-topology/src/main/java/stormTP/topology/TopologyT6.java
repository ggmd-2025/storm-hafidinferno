package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration; // Import direct de Duration
import org.apache.storm.tuple.Fields;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.RankEvolutionBolt;
import stormTP.operator.Exit6Bolt;

public class TopologyT6 {

    public static void main(String[] args) throws Exception {

        // Gestion basique des arguments si non fournis
        int portINPUT = (args.length > 0) ? Integer.parseInt(args[0]) : 9001;
        int portOUTPUT = (args.length > 1) ? Integer.parseInt(args[1]) : 9002;

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Source
        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 2. Calcul des Rangs
        // GiveRankBolt émet : ("id", "top", "nom", "rang", "total", "maxcel")
        builder.setBolt("rank", new GiveRankBolt(), 1)
               .shuffleGrouping("masterStream");
        
        // 3. Evolution (Fenêtre de 10s, glissante toutes les 2s)
        int idToMonitor = 2; // On surveille la tortue ID 2
        
        builder.setBolt("evolution", 
                new RankEvolutionBolt(idToMonitor)
                    // Syntaxe correcte pour une fenêtre basée sur le temps
                    .withWindow(Duration.seconds(10), Duration.seconds(2)), 
                1)
               // fieldsGrouping OBLIGATOIRE : garantit que l'historique de la tortue ID 2 reste dans la même tâche
               .fieldsGrouping("rank", new Fields("id")); 

        // 4. Sortie
        // Exit6Bolt attend : ("id", "top", "nom", "date", "evolution")
        builder.setBolt("exit", new Exit6Bolt(portOUTPUT), 1)
               .shuffleGrouping("evolution"); // Shuffle car le Bolt n'est pas "fieldsGrouping" dependent

        Config config = new Config();
        
        // Si vous utilisez Maven pour soumettre, cette ligne est correcte
        StormSubmitter.submitTopology("topoTP6", config, builder.createTopology());
    }
}