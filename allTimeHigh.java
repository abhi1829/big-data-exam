

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class allTimeHigh {
  	public static class MapClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{
  		public void map(LongWritable key, Text value,Context context)throws IOException,InterruptedException {
  			String[] str = value.toString().split(",");
  			String stk_id = str[1];
  			double high = Double.parseDouble(str[4]);
  			context.write(new Text(stk_id), new DoubleWritable(high));
  		}

 }

  public static class ReduceClass  extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	  
	  public void reduce(Text key, Iterable<DoubleWritable> values,Context context)throws IOException,InterruptedException  {
		  double max = 0;
		  for (DoubleWritable val:values) {
			  if (val.get() > max) {
				  max = val.get();
			  }
		  }
		  context.write(key, new DoubleWritable(max));  
	  }
  }

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "All Time High");
    job.setJarByClass(allTimeHigh.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


