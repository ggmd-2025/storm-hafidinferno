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

public class Exit6Bolt implements IRichBolt {

    private OutputCollector collector;
    private int port;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private BufferedWriter writer;

    public Exit6Bolt(int port) {
        this.port = port;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            this.serverSocket = new ServerSocket(this.port);
            System.out.println("Exit6Bolt listening on port " + this.port);
        } catch (IOException e) {
            e.printStackTrace();
            // Important: Si la préparation échoue, on doit le gérer correctement
        }
    }

    @Override
    public void execute(Tuple t) {
        // S'assurer que le socket est prêt avant d'essayer de se connecter/écrire
        try {
            if (clientSocket == null || clientSocket.isClosed()) {
                // Bloquant : attend qu'un client (ex: votre script python de réception) se connecte
                clientSocket = serverSocket.accept(); 
                writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                System.out.println("Client connecté à Exit6Bolt.");
            }

            int id = t.getIntegerByField("id");
            int top = t.getIntegerByField("top");
            String nom = t.getStringByField("nom");
            String date = t.getStringByField("date");
            String evolution = t.getStringByField("evolution");

            // Format JSON de sortie
            String json = String.format(
                "{\"id\":%d,\"top\":%d,\"nom\":\"%s\",\"date\":\"%s\",\"evolution\":\"%s\"}",
                id, top, nom, date, evolution
            );

            writer.write(json);
            writer.newLine();
            writer.flush();
            
            System.out.println("EXIT6 >>> " + json);

            // Émettre pour la trace ou pour d'autres Bolts si nécessaire (ici, pas vraiment utile)
            collector.emit(new Values(json));
            collector.ack(t);

        } catch (IOException e) {
            System.err.println("Erreur de communication Exit6Bolt : " + e.getMessage());
            // Fermer les ressources et les réinitialiser
            try {
                if (clientSocket != null) clientSocket.close();
            } catch (IOException closeEx) {
                // Ignorer
            }
            clientSocket = null;
            writer = null;
            collector.fail(t); // Signale que le tuple n'a pas été traité
        }
    }

    @Override
    public void cleanup() {
        try {
            if (writer != null) writer.close();
            if (clientSocket != null) clientSocket.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
             System.err.println("Erreur lors du cleanup d'Exit6Bolt: " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Émet le JSON pour la sortie ou les logs
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}