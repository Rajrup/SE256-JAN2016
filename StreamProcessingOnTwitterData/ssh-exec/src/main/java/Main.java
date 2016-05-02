import java.io.*;

/**
 * Created by robin on 1/5/16.
 */
public class Main {
        public String executeCommand(String command) {
            String country;
            double count;
            long total;
            StringBuffer output = new StringBuffer();

            Process p;
            try {
                p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader reader1 =
                        new BufferedReader(new InputStreamReader(p.getErrorStream()));
                String line = "";
                while ((line = reader.readLine())!= null) {
                    String[] coloumns=line.split("\\|");
                    if(coloumns.length==3) {
                        country = coloumns[0];
                        count = Double.parseDouble(coloumns[1]);
                        total = Long.parseLong(coloumns[2]);
                        //output.append(line + "\n");
                        System.out.print(country + " " + count + " " + total + "\n");
                        client.loadData(country,count,total);
                    }else{
                        System.out.println("Line is: "+line);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            return output.toString();

        }
        private Cassandra client;
        public static void main(String[] args) throws IOException {
            Main m=new Main();
            m.client=new Cassandra();
            m.client.connect("127.0.0.1");
            m.client.createSchema();
           m.executeCommand(" ssh dnithinraj@se256-project4-ssh.azurehdinsight.net "+
                    "\"java -cp print.jar Main sentiment_data\"");
                    //"\"ls /usr\""));
        }
}
