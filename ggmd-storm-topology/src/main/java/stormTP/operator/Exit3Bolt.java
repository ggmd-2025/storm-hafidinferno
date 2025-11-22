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

public class Exit3Bolt implements IRichBolt {

    private OutputCollector collector;
    
    // Variables pour la gestion de la Socket
    private int port;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedWriter writer;

    public Exit3Bolt(int port) {
        this.port = port;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            // 1. Ouverture du port serveur (une seule fois)
            this.serverSocket = new ServerSocket(this.port);
            System.out.println("Exit3Bolt listening on port " + this.port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple t) {
        try {
            // 2. Connexion au Listener si nécessaire
            if (clientSocket == null || clientSocket.isClosed()) {
                System.out.println("Waiting for listener connection...");
                clientSocket = serverSocket.accept(); // Bloquant jusqu'à connexion
                writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                System.out.println("Listener connected!");
            }

            // Récupération des données
            int id = t.getIntegerByField("id");
            int top = t.getIntegerByField("top");
            String rang = t.getStringByField("rang");
            int total = t.getIntegerByField("total");
            int maxcel = t.getIntegerByField("maxcel");

            // Formatage JSON
            String json = String.format(
                "{\"id\":%d,\"top\":%d,\"rang\":\"%s\",\"total\":%d,\"maxcel\":%d}",
                id, top, rang, total, maxcel
            );

            // 3. Envoi des données
            writer.write(json);
            writer.newLine(); // Indispensable pour le client
            writer.flush();
            
            System.out.println("EXIT3 >>> " + json);

            // Acquittement à Storm
            collector.emit(new Values(json));
            collector.ack(t);

        } catch (IOException e) {
            System.err.println("Erreur d'écriture (Listener déconnecté ?) : " + e.getMessage());
            // Réinitialisation pour permettre une reconnexion
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { 
        return null; 
    }
}