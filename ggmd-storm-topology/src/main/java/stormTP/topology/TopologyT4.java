package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.PointsBolt;
import stormTP.operator.Exit4Bolt;

public class TopologyT4 {

    public static void main(String[] args) throws Exception {

        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Spout
        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 2. Calcul du Rang
        builder.setBolt("rank", new GiveRankBolt(), 1)
               .shuffleGrouping("masterStream");
        
        // 3. Calcul des Points (reçoit le flux de "rank")
        builder.setBolt("points", new PointsBolt(), 1)
               .shuffleGrouping("rank");

        // 4. Sortie (reçoit le flux de "points")
        builder.setBolt("exit", new Exit4Bolt(portOUTPUT), 1)
               .shuffleGrouping("points");

        Config config = new Config();
        StormSubmitter.submitTopology("topoTP4", config, builder.createTopology());
    }
}