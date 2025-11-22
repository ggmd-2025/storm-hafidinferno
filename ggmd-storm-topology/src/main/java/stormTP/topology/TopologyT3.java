package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.Exit3Bolt;

public class TopologyT3 {

    public static void main(String[] args) throws Exception {

        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        builder.setBolt("rank", new GiveRankBolt(), 1)
               .shuffleGrouping("masterStream");

        builder.setBolt("exit", new Exit3Bolt(portOUTPUT), 1)
               .allGrouping("rank");     // <---- LA SEULE LIGNE IMPORTANTE

        Config config = new Config();

        StormSubmitter.submitTopology("topoTP3", config, builder.createTopology());
    }
}
