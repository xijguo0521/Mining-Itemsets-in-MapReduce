import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MRMiner {


    public static boolean executeFirstRound(String inputPath, String firstOutputPath, int minSupp, int corr,
                                            int transactionsPerBlock)
            throws Exception {
        Job firstJob = createJob("MR first round", MRFirstRound.class,
                MRFirstRound.FirstMapper.class, MRFirstRound.FirstReducer.class,
                Text.class, IntWritable.class, Text.class, NullWritable.class);
        // Set the number of transactions that are given in input to each invocation of the first Map function

        firstJob.setInputFormatClass(MultiLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(firstJob, transactionsPerBlock);

        firstJob.getConfiguration().setInt("MINSUPP", minSupp);
        firstJob.getConfiguration().setInt("CORR", corr);
        firstJob.getConfiguration().setInt("TRANS_PER_BLOCK", transactionsPerBlock);

        int totalNumTransactions = getTotalNumTransactions(inputPath);
        firstJob.getConfiguration().setInt("TOTAL_NUM_TRANS", totalNumTransactions);

        FileInputFormat.setInputPaths(firstJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(firstJob, new Path(firstOutputPath));
        return firstJob.waitForCompletion(true);
    }

    public static boolean executeSecondRound(String inputPath, String firstOutputPath, String finalOutputPath,
                                             int minSupp, int transactionsPerBlock) throws Exception {
        Job secondJob = createJob("MR second round", MRSecondRound.class,
                MRSecondRound.SecondMapper.class, MRSecondRound.SecondReducer.class,
                Text.class, IntWritable.class, Text.class, NullWritable.class);

        // Set the number of transactions that are given in input to each invocation of the first Map function

        secondJob.setInputFormatClass(MultiLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(secondJob, transactionsPerBlock);

        secondJob.getConfiguration().setInt("MINSUPP", minSupp);
        secondJob.addCacheFile(new Path(firstOutputPath).toUri());
        FileInputFormat.setInputPaths(secondJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(secondJob, new Path(finalOutputPath));
        return secondJob.waitForCompletion(true);
    }

    public static Job createJob(String jobName, Class mainClass, Class mapperClass,
                                Class reducerClass, Class mapOutputKeyClass,
                                Class mapOutputValueClass, Class outputKeyClass,
                                Class outputValueClass) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);

        return job;
    }

    private static int getTotalNumTransactions(String inputDirPath) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(inputDirPath), true);

        int totalNumTransactions = 0;

        while (iter.hasNext()) {
            System.out.println("should only enter once");
            LocatedFileStatus fileStatus = iter.next();
            String pn = fileStatus.getPath().toString();
            System.out.println("file name is: " + pn);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pn))));
            String line = null;
            while ((line = br.readLine()) != null) {
                totalNumTransactions++;
            }
        }
        return totalNumTransactions;
    }

    public static void main(String[] args) throws Exception {
        int minSupp = Integer.parseInt(args[0]);
        int corr = Integer.parseInt(args[1]);
        int transactionsPerBlock = Integer.parseInt(args[2]);
        String inputPath = args[3];
        String firstOutputPath = args[4];
        String finalOutputPath = args[5];

        boolean firstRoundComplete = executeFirstRound(inputPath, firstOutputPath, minSupp, corr, transactionsPerBlock);

        if (firstRoundComplete)
            executeSecondRound(inputPath, firstOutputPath, finalOutputPath, minSupp, transactionsPerBlock);
    }
}
