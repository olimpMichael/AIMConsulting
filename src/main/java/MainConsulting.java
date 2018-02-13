import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MainConsulting {
    private volatile Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    private String csvSplit = ";";

    public static void main(String[] args) {
        final MainConsulting mainConsulting = new MainConsulting();

        ExecutorService service = Executors.newFixedThreadPool(10);
        for(final String path : args){
            service.execute(new Runnable() {
                @Override
                public void run() {
                    BufferedReader bufferedReader = null;
                    try {
                        bufferedReader = new BufferedReader(new FileReader(path));
                        String[] keys = bufferedReader.readLine().split(mainConsulting.csvSplit);
                        String str;

                        for (int i = 0; i < keys.length; i++){
                            if (!mainConsulting.map.containsKey(keys[i]))
                            mainConsulting.map.put(keys[i], new HashSet<String>());
                        }
                        while((str = bufferedReader.readLine()) != null){
                            String[] values = str.split(mainConsulting.csvSplit);
                            for (int i = 0; i < keys.length; i++){
                                synchronized (mainConsulting.map) {
                                    Set<String> setValues = mainConsulting.map.get(keys[i]);
                                    setValues.add(values[i]);
                                    mainConsulting.map.put(keys[i], setValues);
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally{
                        if (bufferedReader != null) {
                            try {
                                bufferedReader.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        Iterator<Map.Entry<String, Set<String>>> iterator = mainConsulting.map.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String, Set<String>> entry = iterator.next();
            String fileName = entry.getKey() + ".csv";
            Set<String> valuesSet = entry.getValue();
            File file = new File(fileName);
            BufferedWriter bufferedWriter = null;
            try {
                bufferedWriter = new BufferedWriter(new FileWriter(file));
                for(String str: valuesSet) {
                    bufferedWriter.write(str + mainConsulting.csvSplit);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally{
                if (bufferedWriter != null){
                    try {
                        bufferedWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
