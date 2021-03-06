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

		private static IntWritable zero = new IntWritable(0);
		private static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			String start = strs[0];
			String[] ends = strs[1].split("#");
			String str = null;
			for (String end : ends) {
				str = start.split("@")[1] + "#" + end.split("@")[1];
				context.write(new Text(str), zero);
			}
			for (int i = 0; i < ends.length - 1; i++) {
				String[] str0 = ends[i].split("@");
				int d0 = Integer.parseInt(str0[0]);
				String s0 = str0[1];
				for (int j = i + 1; j < ends.length; j++) {
					String[] str1 = ends[j].split("@");
					int d1 = Integer.parseInt(str1[0]);
					String s1 = str1[1];
					if (d0 > d1) {
						str = s1 + "#" + s0;
					} else if (d0 < d1) {
						str = s0 + "#" + s1;
					} else if (s0.compareTo(s1) > 0) {
						str = s1 + "#" + s0;
					} else if (s0.compareTo(s1) < 0) {
						str = s0 + "#" + s1;
					} else {
						System.out.println("single circle");
					}
					context.write(new Text(str), one);
				}
			}
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
