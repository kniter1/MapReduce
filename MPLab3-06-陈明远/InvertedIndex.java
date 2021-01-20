import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                Text word = new Text();
                String temp = itr.nextToken();
                if(temp.equals("风雪")) {
                    if (filename.indexOf(".txt.segmented") > 0) {
                        word.set(temp+ "," + filename.substring(0, filename.indexOf(".txt.segmented")));
                        context.write(word, new IntWritable(1));
                    } else {
                        word.set(temp + "," + filename.substring(0, filename.indexOf(".TXT.segmented")));
                        context.write(word, new IntWritable(1));
                    }
                }
            }
        }

    }

    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = new String();
            term = key.toString().split(",")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text word1 = new Text();
        private Text word2 = new Text();
        String bookname = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split(",")[0]);
            bookname = key.toString().split(",")[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            word2.set(bookname + ":" + sum);
            if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
                StringBuilder out = new StringBuilder();
                long count = 0;
                for (String p : postingList) {
                    out.append(p);
                    out.append(";");
                    count += Long.parseLong(p.substring(p.indexOf(":") + 1));
                }
                StringBuilder out1 = new StringBuilder(String.format("%.2f", ((double) count / postingList.size()))).append("," + out);
                if (count > 0) {
                    context.write(word1, new Text(out1.substring(0, out1.lastIndexOf(";")).toString()));
                }
                postingList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            long count = 0;
            for (String p : postingList) {
                out.append(p);
                out.append(";");
                count += Long.parseLong(p.substring(p.indexOf(":") + 1));
            }
            StringBuilder out1 = new StringBuilder(String.format("%.2f", ((double) count / postingList.size()))).append("," + out);
            if (count > 0) {
                context.write(word1, new Text(out1.substring(0, out1.lastIndexOf(";")).toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:hadoop jar InvertedIndex.jar InvertedIndex main input output");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

