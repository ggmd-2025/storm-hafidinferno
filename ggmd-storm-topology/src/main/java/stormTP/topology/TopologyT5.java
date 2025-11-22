package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.SpeedBolt;
import stormTP.operator.Exit5Bolt;

public class TopologyT5 {

    public static void main(String[] args) throws Exception {

        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Source
        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 2. Calcul de la Vitesse avec fenêtrage
        // Fenêtre de longueur 10 (Count.of(10)) qui avance de 5 (Count.of(5))
        builder.setBolt("speed", 
                new SpeedBolt()
                    .withWindow(BaseWindowedBolt.Count.of(10), BaseWindowedBolt.Count.of(5)), 
                1)
               .shuffleGrouping("masterStream");

        // 3. Sortie
        builder.setBolt("exit", new Exit5Bolt(portOUTPUT), 1)
               .shuffleGrouping("speed");

        Config config = new Config();
        StormSubmitter.submitTopology("topoTP5", config, builder.createTopology());
    }
}