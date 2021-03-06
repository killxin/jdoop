package mainclass;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedSorter {

	public static class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
		private Text disc = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String temp = value.toString().split(",")[0];
			String loc = value.toString().split(",")[1];
			Double fre = Double.parseDouble(temp.split("\t")[1]);
			disc.set(temp.split("\t")[0] + "#" + loc);
			context.write(new DoubleWritable(fre), disc);
		}
	}

	public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text disc = new Text();

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String fre = key.toString();
			for (Text val : values) {
				word.set(val.toString().split("#")[0]);
				String temp = val.toString().split("#")[1];
				disc.set(fre + "," + temp);
				context.write(word, disc);
			}
		}
	}

	private static class DoubleWritableDecreasingComparator extends DoubleWritable.Comparator {
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "invert result sort");
		job.setJarByClass(InvertedSorter.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setSortComparatorClass(DoubleWritableDecreasingComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
