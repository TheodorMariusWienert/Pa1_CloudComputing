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

  public static class Entry
    {
      public  String word;
      public  int count;

    public Entry(String word2, int count2 )
    {
        word=word2;
        count=count2;
    }

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


      SortedSet<Entry> tree;
        @Override
      protected void setup(Context context)
                  throws IOException,
                         InterruptedException{

                           tree  = new TreeSet<>(new Comparator<Entry>() {
                                    @Override
                                    public int compare(Entry s1, Entry s2) {
                                        return s1.count-s2.count;
                                    }
                                });
                         }
                @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        int sum = 0;
                        for (IntWritable val : values) {
                                sum += val.get();
                        }

        tree.add(new Entry( key.toString(),-sum));
        //tree.pollLastEntry();
              }




        @Override
            protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
                        throws IOException,
                               InterruptedException{
                                int count =0;
                                 for (Entry element : tree) {
                                   context.write(new Text(element.word()), new IntWritable(-element.count()));
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



           }
