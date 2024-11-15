import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class MRFirstRound {

    public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            List<Set<Integer>> dataset = new ArrayList<>();
            String[] lines = input.split("\n");

            // Process data from input and store them as transactions in a dataset
            for(String line : lines) {
                String[] items = line.split(" ");
                Set<Integer> transaction = new HashSet<>();

                for (String item : items)
                    transaction.add(Integer.parseInt(item));

                dataset.add(transaction);
            }

            // Use the formula (MinSupp - CORR) * TRANS_PER_BLOCK / Total Number of Transactions as the adjusted minSupp
            int minSupp = context.getConfiguration().getInt("MINSUPP", 0);
            int corr = context.getConfiguration().getInt("CORR", 0);
            int totalNumTransactions = context.getConfiguration().getInt("TOTAL_NUM_TRANS", 0);
            int transPerBlock = context.getConfiguration().getInt("TRANS_PER_BLOCK", 0);
            int minSuppCorr = (minSupp - corr) * transPerBlock / totalNumTransactions;

            MRApriori mrApriori = new MRApriori();
            mrApriori.mineFIs(dataset, minSuppCorr);
            List<Set<Integer>> freqItemsets = mrApriori.getFreqItemsets();

            for (Set<Integer> fi : freqItemsets) {
                List<Integer> fiList = new ArrayList<>(fi);
                Collections.sort(fiList);

                String keyOut = "";
                for (Integer i : fiList)
                    keyOut += String.valueOf(i) + " ";

                context.write(new Text(keyOut.trim()), new IntWritable(1));
            }
        }
    }

    public static class FirstReducer extends Reducer<Text, IntWritable,
            Text, NullWritable> {

        public void reduce(Text text, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            context.write(new Text(text.toString()), NullWritable.get());
        }
    }
}
