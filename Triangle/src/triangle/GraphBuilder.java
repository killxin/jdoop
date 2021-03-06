package triangle;

import java.util.HashMap;
import java.util.TreeSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

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

		private static HashMap<String, Integer> degree_map = new HashMap<String, Integer>();
		private static Path localFiles = null;

		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFile = context.getCacheFiles();
			localFiles = new Path(cacheFile[0]);
			String line;
			BufferedReader br = new BufferedReader(new FileReader(localFiles.toString()));
			while ((line = br.readLine()) != null) {
				String[] strs = line.split("#");
				if (strs.length != 2) {
					System.err.println(line);
				} else {
					degree_map.put(strs[0], Integer.parseInt(strs[1]));
				}
			}
			br.close();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			if (strs.length != 2) {
				System.out.println("illegal input");
			}
			int d0 = 0, d1 = 0;
			if (degree_map.containsKey(strs[0])) {
				d0 = degree_map.get(strs[0]);
			}
			if (degree_map.containsKey(strs[1])) {
				d1 = degree_map.get(strs[1]);
			}
			String s0 = d0 + "@" + strs[0];
			String s1 = d1 + "@" + strs[1];
			if (d0 > d1) {
				context.write(new Text(s1), new Text(s0));
			} else if (d0 < d1) {
				context.write(new Text(s0), new Text(s1));
			} else if (strs[0].compareTo(strs[1]) > 0) {
				context.write(new Text(s1), new Text(s0));
			} else if (strs[0].compareTo(strs[1]) < 0) {
				context.write(new Text(s0), new Text(s1));
			} else {
				System.out.println("single circle");
			}
		}
	}

	public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeSet<String> neigh = new TreeSet<String>();
			for(Text value : values){
				neigh.add(value.toString());
			}
			StringBuilder str = new StringBuilder();
			for (String val : neigh) {
				str.append(val.toString());
				str.append("#");
			}
			context.write(key, new Text(str.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Graph Builder");
		job1.addCacheFile(new Path("degree_sort.txt").toUri());
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
