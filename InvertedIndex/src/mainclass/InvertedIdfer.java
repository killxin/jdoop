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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import mainclass.InvertedSorter.SortMapper;
import mainclass.InvertedSorter.SortReducer;

public class InvertedIdfer {

	public static class IDFMapper extends Mapper<Object, Text, Text, Text> {
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			计算该词所在的所有文档数idf
			for循环：
				发现一个新作家，计算该词在新作家的作品中出现的次数tf
			 	输出的key是作家的名字，value是词语#tf#idf
			 */
		}
	}

	public static class IDFReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			/*
			 for循环：
			 	将values拼起来
			 */
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "invert result analize");
		job.setJarByClass(InvertedIdfer.class);
		job.setMapperClass(IDFMapper.class);
		job.setReducerClass(IDFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
