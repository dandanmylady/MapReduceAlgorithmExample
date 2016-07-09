package lyndon.my_mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class InMapCombiner
{
    public static class MyWordCounterMapper extends Mapper<LongWritable, Text, Text, VIntWritable>
    {
       // private final static VIntWritable one = new VIntWritable(1);
        private Text word = new Text();
        private HashMap<String, Integer> words = new HashMap<String, Integer>();

        protected void cleanup(Context context)
            throws IOException, InterruptedException
        {
            for(String key : words.keySet())
            {
                word.set(key);
                context.write(word, new VIntWritable(words.get(key)));
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                String tmp = itr.nextToken();
                tmp = tmp.replaceAll("[^a-zA-Z0-9]", " ");
                for(String s : tmp.split(" +")) {
                    Integer freq = words.get(s.toLowerCase());
                    words.put(s.toLowerCase(), (freq == null) ? 1 : (freq + 1) );
                }
            }
        }
    }

    public static class MyWordCounterReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>
    {
        private VIntWritable result = new VIntWritable();
        public void reduce(Text key, Iterable<VIntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            for(VIntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "my word counter");
        job.setJarByClass(MyWordCounter.class);
        job.setMapperClass(MyWordCounterMapper.class);
        job.setReducerClass(MyWordCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
