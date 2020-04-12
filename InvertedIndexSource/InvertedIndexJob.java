import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.io.File;
import java.nio.file.*;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndexJob
{

        public static void main(String[] args)
                throws IOException, ClassNotFoundException, InterruptedException
        {
                if (args.length != 2)
                {
                        System.err.println("Usage: Inverted Index <input path> <output path>");
                        System.exit(-1);
                }
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "Inverted Index");
                job.setJarByClass(InvertedIndexJob.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileInputFormat.setInputDirRecursive(job, true);
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setMapperClass(InvertedIndexMapper.class);
                job.setReducerClass(InvertedIndexReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.waitForCompletion(true);

        }

        public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
        {

                private Text word = new Text();

                public void map(LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException
                {
                        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                        String line = value.toString();

                        StringTokenizer tokenizer = new StringTokenizer(line.trim(), " -,;:(){}[]\t\n\r\f");
                        while(tokenizer.hasMoreTokens())
                        {
                                word.set(tokenizer.nextToken().replaceAll("[^\\s\\p{L}\\p{N}']|(?<=(^|\\s))'|'(?=($|\\s))", "").toLowerCase());
                                if (!word.toString().equals("") && !word.toString().isEmpty())
                                {
                                        context.write(word, new Text(fileName));
                                }
                        }
                }
        }

        public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
        {
                public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException
                {
                        HashMap<String, Integer> map = new HashMap<String, Integer>();
                        int count = 0;
                        for (Text val : values)
                        {
                                String str = val.toString();
                                if (map != null && map.get(str) != null)
                                {
                                        count = (int)map.get(str);
                                        map.put(str, ++count);
                                }
                                else
                                {
                                        map.put(str, 1);
                                }
                        }
                        context.write(key, new Text(map.toString()));
                }
        }

}