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

      TreeSet<Map.Entry<String, Integer>> tree;
        @Override
      protected void setup(Context context)
                  throws IOException,
                         InterruptedException{

                           tree =new TreeSet<>(new Comparator<Map.Entry<String, Integer>>(){
                              @Override
                              public int compare(Map.Entry<String, Integer> me1, Map.Entry<String, Integer> me2) {
                                  int temp=me1.getValue().compareTo(me2.getValue());
                                  if(temp==0)  return me1.getKey().compareTo(me2.getKey());
                                  else return temp;
                              }   });
                            }
                @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        int sum = 0;
                        for (IntWritable val : values) {
                                sum += val.get();
                        }

                         Map.Entry<String,Integer> entry =
                     new AbstractMap.SimpleEntry<String, Integer>(key.toString(), -sum);
                     if(tree.size()<10)tree.add(entry);
                     else{
                       tree.add(entry);
                       tree.pollLast()
                       
                     }

        //tree.pollLastEntry();
              }




        @Override
            protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
                        throws IOException,
                               InterruptedException{
                                  int count=0;
                                 for(Map.Entry<String,Integer> element : tree) {
                                   context.write(new Text(element.getKey()), new IntWritable(-element.getValue()));
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
