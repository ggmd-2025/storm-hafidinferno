package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.Exit2Bolt;

public class TopologyT2 {

    public static void main(String[] args) throws Exception {

        int nbExecutors = 1;

        if (args.length != 2) {
            System.out.println("Usage: TopologyT2 <portInput> <portOutput>");
            return;
        }

        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("masterStream", spout);

        builder.setBolt("filterTortoise",
                new MyTortoiseBolt(3, "Raphaelo"), nbExecutors)
                .shuffleGrouping("masterStream");

        builder.setBolt("exit",
                new Exit2Bolt(portOUTPUT), nbExecutors)
                .shuffleGrouping("filterTortoise");

        Config config = new Config();

        // --- SOUMISSION VIA STORM CLI ---
        StormSubmitter.submitTopology("topoTP2", config, builder.createTopology());
    }
}
