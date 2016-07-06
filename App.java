package lyndon.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class App
{
    public static class WordCountMapper
        extends Mapper<LongWritable, Text, Text, VIntWritable>
    {

        private final static VIntWritable one = new VIntWritable(1);
        private Text word = new Text();


        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }


    public static class WordCountReducer
        extends Reducer<Text, VIntWritable, Text, VIntWritable>
    {
        private VIntWritable result = new VIntWritable();

        @Override
        public void reduce(Text key, Iterable<VIntWritable> values, Context context)
            throws IOException, InterruptedException
        {
            int sum = 0;
            for(VIntWritable val : values){
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(App.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
