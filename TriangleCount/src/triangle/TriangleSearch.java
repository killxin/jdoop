package triangle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleSearch {

	public static class TriangleSearchMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			int num = Integer.parseInt(strs[1]);
			context.write(new Text(strs[0]), new IntWritable(num));		
		}
	}

	public static class TriangleSearchCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		private static IntWritable result = new IntWritable();
		private static IntWritable zero = new IntWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			boolean sign = false;
			int sum = 0, val = 0;
			for (IntWritable value : values) {
				val = value.get();
				if (val == 0) {
					sign = true;
				} else {
					sum += val;
				}
			}
			if (sign) {
				context.write(key, zero);
			}
			if (sum != 0) {
				result.set(sum);
				context.write(key, result);
			}
		}
	}

	public static class TriangleSearchReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private static Text current_num = new Text("num");

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			boolean sign = false;
			int sum = 0, val = 0;
			for (IntWritable value : values) {
				val = value.get();
				if (val == 0) {
					sign = true;
				} else {
					sum += val;
				}
			}
			if (sign && sum != 0) {
				Configuration conf = context.getConfiguration();
				int num = conf.getInt("triangle", 0) + sum;
				conf.setInt("triangle", num);
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int num = conf.getInt("triangle", 0);
			context.write(current_num, new IntWritable(num));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("triangle", 0);
		Job job1 = Job.getInstance(conf, "Triangle Search");
		job1.setJarByClass(TriangleSearch.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(TriangleSearchMapper.class);
		job1.setReducerClass(TriangleSearchReducer.class);
		job1.setCombinerClass(TriangleSearchCombiner.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
	}

}
