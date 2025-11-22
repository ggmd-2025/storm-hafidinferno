package stormTP.operator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Exit5Bolt implements IRichBolt {

    private OutputCollector collector;
    private int port;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedWriter writer;

    public Exit5Bolt(int port) {
        this.port = port;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.serverSocket = new ServerSocket(this.port);
            System.out.println("Exit5Bolt listening on port " + this.port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple t) {
        try {
            if (clientSocket == null || clientSocket.isClosed()) {
                clientSocket = serverSocket.accept();
                writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            }

            // Récupération des données envoyées par SpeedBolt
            int id = t.getIntegerByField("id");
            String nom = t.getStringByField("nom");
            String tops = t.getStringByField("tops");
            double vitesse = t.getDoubleByField("vitesse");

            // Construction du JSON attendu
            String json = String.format(
                "{\"id\":%d,\"nom\":\"%s\",\"tops\":\"%s\",\"vitesse\":%.2f}",
                id, nom, tops, vitesse
            );

            writer.write(json);
            writer.newLine();
            writer.flush();
            
            System.out.println("EXIT5 >>> " + json);

            collector.emit(new Values(json));
            collector.ack(t);

        } catch (IOException e) {
            System.err.println("Erreur Exit5Bolt : " + e.getMessage());
            clientSocket = null;
            writer = null;
            collector.fail(t);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (writer != null) writer.close();
            if (clientSocket != null) clientSocket.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}