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
     List<String> commonWords = Arrays.asList("the", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not");
     @Override
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         StringTokenizer tokenizer = new StringTokenizer(line, " \t,;.?!-:@[](){}_*/");
         while (tokenizer.hasMoreTokens()) {
             String nextToken = tokenizer.nextToken();
             if (!commonWords.contains(nextToken.trim().toLowerCase())) {
                 context.write(new Text(nextToken), new IntWritable(1));
             }
         }
     }
 }

    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

      TreeMap<String, String> tree;
        @Override
      protected void setup(Context context)
                  throws IOException,
                         InterruptedException{

                           tree =  new TreeMap<String, String>();
                         }
                @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        int sum = 0;
                        for (IntWritable val : values) {
                                sum += val.get();
                        }

        tree.put((-sum).toString()+"/"key.toString()), key.toString());
        //tree.pollLastEntry();
              }




        @Override
            protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
                        throws IOException,
                               InterruptedException{

                                 for(Map.Entry<Integer,String> element : tree.entrySet()) {
                                   String[] arrOfStr = element.getKey().toString().split("/", 2);
                                   int number = Integer.parseInt(arrOfStr[0]);	
                                   context.write(new Text(element.getValue()), new IntWritable(number));
                                        count++;
                                if(count==10)break;
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
