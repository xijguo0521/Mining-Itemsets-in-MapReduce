import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.util.StringUtils;
import java.net.URI;

public class MRSecondRound {

    public static class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Configuration conf;
        private Map<Set<Integer>, Integer> freqItemsetsSupp = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            conf = context.getConfiguration();
            URI[] firstRoundOutputFiles = Job.getInstance(conf).getCacheFiles();
            for (URI firstRoundOutputFile : firstRoundOutputFiles) {
                    Path firstRoundOutputPath = new Path(firstRoundOutputFile.getPath());
                    RemoteIterator<LocatedFileStatus> iter = FileSystem.get(context.getConfiguration()).listFiles(
                            new Path(firstRoundOutputPath.getName()), true);

                    while (iter.hasNext()) {
                        LocatedFileStatus fileStatus = iter.next();
                        String pathName = fileStatus.getPath().toString();
                        if (pathName.contains("part-r-")) {
                            parseFile(pathName, context);
                        }
                    }
            }
        }

        private void parseFile(String pathName, Context context) {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        FileSystem.get(context.getConfiguration()).open(new Path(pathName))));
                String transactions = null;

                while ((transactions = br.readLine()) != null) {

                    String[] input = transactions.split("\\s+");
                    Set<Integer> freqItemset = new HashSet<>();

                    for (String str : input)
                        freqItemset.add(Integer.parseInt(str));

                    freqItemsetsSupp.put(freqItemset, 0);

                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file"
                        + StringUtils.stringifyException(ioe));
            }

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String[] lines = input.split("\n");

            for (String line : lines) {
                System.out.println("every line: " + line);
                String[] items = line.split(" ");
                Set<Integer> t = new HashSet<>();
                for (String item : items)
                    t.add(Integer.parseInt(item));

                for (Map.Entry<Set<Integer>, Integer> entry : freqItemsetsSupp.entrySet()) {
                    if (t.containsAll(entry.getKey()))
                        freqItemsetsSupp.put(entry.getKey(), freqItemsetsSupp.getOrDefault(entry.getKey(), 0) + 1);
                }
            }

            for (Map.Entry<Set<Integer>, Integer> entry : freqItemsetsSupp.entrySet()) {
                System.out.println("each entry key: " + entry.getKey() + "each entry value: " + entry.getValue());
                List<Integer> l = new ArrayList<>(entry.getKey());
                Collections.sort(l);

                String keyOut = "";
                for (Integer i : l)
                    keyOut += String.valueOf(i) + " ";

                context.write(new Text(keyOut), new IntWritable(entry.getValue().intValue()));
            }
        }
    }

    public static class SecondReducer extends Reducer<Text, IntWritable,
            Text, NullWritable> {

        public void reduce(Text freqItemset, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int minSupp = context.getConfiguration().getInt("MINSUPP", 0);
            int totalSupp = 0;

            for (IntWritable val : values) {
                totalSupp += val.get();
            }

            if (totalSupp >= minSupp) {
                String keyOut = freqItemset.toString() + String.valueOf(totalSupp);
                context.write(new Text(keyOut), NullWritable.get());
            }
        }
    }
}
