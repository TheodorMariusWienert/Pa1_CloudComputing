import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
    public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String nextToken = tokenizer.nextToken();
				context.write(new Text(nextToken), new IntWritable(1));
			}
		}
	}

    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      int count;
       TreeMap<Integer, String> tree;
      protected void setup(Context context)
                  throws IOException,
                         InterruptedException{
                           count=0;
                           tree =  new TreeMap<Integer, String>();
                         }
		@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
      if(count<10){
         tree.put(-sum, key);
         count++;
      }
      else {
        tree.put(-sum, key);
        tree.pollFirstEntry();
      }


			context.write(key, new IntWritable(sum));
		}
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException,
                       InterruptedException{
                         for(Map.Entry<Integer,String> element : treeMap.entrySet()) {
                           context.write(element.getValue(), new IntWritable(-element.getKey()));
                         }
    }


	}

	public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "wordcount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(WordCount.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
