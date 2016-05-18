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
			if (strs.length != 2) {
				System.out.println("illegal input");
				System.out.println(value.toString());
			}
			String start = strs[0];
			String[] ends = strs[1].split("#");
			String str = null;
			for (String end : ends) {
				str = start + "#" + end;
				context.write(new Text(str), zero);
			}
			for (int i = 0; i < ends.length - 1; i++) {
				str = ends[i + 1] + "#" + ends[i];
				context.write(new Text(str), one);
			}
		}
	}

	public static class TriangleSearchReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			boolean sign = false;
			int sum = 0;
			for (IntWritable value : values) {
				int val = value.get();
				if (val == 0) {
					sign = true;
				} else {
					sum += val;
				}
			}
			if (sign) {
				context.write(key, new IntWritable(sum));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Triangle Search");
		job1.setJarByClass(TriangleSearch.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(TriangleSearchMapper.class);
		job1.setReducerClass(TriangleSearchReducer.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
	}

}
