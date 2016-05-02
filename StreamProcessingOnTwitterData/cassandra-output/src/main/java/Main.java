import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by robin on 1/5/16.
 */
public class Main {
    private Cassandra client;
    public static void main(String[] args){
        new Main().run(args);


    }

    private void run(String[] args) {
        Integer time=(args.length>2)?Integer.parseInt(args[1]):1000;
        Timer timer = new Timer();
        client=new Cassandra();
        client.connect("127.0.0.1");
        timer.schedule(new SayHello(args[0]), 0, time);
    }

    class SayHello extends TimerTask {
        private final String table;

        public SayHello(String table){
            this.table=table;
        }
        public void run() {
            client.printData(table);
        }
    }


}
