package triangle;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder {

	public static class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			if (strs.length != 2) {
				System.out.println("illegal input");
			}
			if (strs[0].compareTo(strs[1]) < 0) {
				context.write(new Text(strs[1]), new Text(strs[0]));
			} else if (strs[0].compareTo(strs[1]) > 0) {
				context.write(new Text(strs[0]), new Text(strs[1]));
			} else {
				System.out.println("single circle");
			}
		}
	}

	public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Set<String> neigh = new TreeSet<String>();
			for (Text value : values) {
				neigh.add(value.toString());
			}
			Iterator<String> ite = neigh.iterator();
			StringBuilder str = new StringBuilder();
			while (ite.hasNext()) {
				str.append(ite.next());
				str.append("#");
			}
			context.write(key, new Text(str.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Graph Builder");
		job1.setJarByClass(GraphBuilder.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(GraphBuilderMapper.class);
		job1.setReducerClass(GraphBuilderReducer.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
	}
}