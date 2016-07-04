package relationgraph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelationGraph {
	
	public static class RelationMapper extends Mapper<LongWritable, Text, Text, Text> {

		private static Path localFiles = null;

		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFile = context.getCacheFiles();
			localFiles = new Path(cacheFile[0]);
			String line;
			BufferedReader br = new BufferedReader(new FileReader(localFiles.toString()));
			while ((line = br.readLine()) != null) {
				UserDefineLibrary.insertWord(line, "nameDic", 1000);
			}
			br.close();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> persons = new HashSet<String>();
			Result res = DicAnalysis.parse(value.toString());
			for (Term t : res.getTerms()) {
				if (t.getNatureStr().compareTo("nameDic") == 0) {
					persons.add(t.getName());
				}
			}
			List<String> args = new ArrayList<String>(persons);
			for (int i = 0; i < args.size(); i++) {
				for (int j = i + 1; j < args.size(); j++) {
					context.write(new Text(args.get(i)), new Text(args.get(j) + "#1"));
					context.write(new Text(args.get(j)), new Text(args.get(i) + "#1"));
				}
			}
		}
	}

	public static class RelationCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<String, Double>();
			for (Text val : values) {
				String[] res = val.toString().split("#");
				if (map.containsKey(res[0])) {
					map.put(res[0], map.get(res[0]) + Double.parseDouble(res[1]));
				} else {
					map.put(res[0], Double.parseDouble(res[1]));
				}
			}
			for (Entry<String, Double> entry : map.entrySet()) {
				context.write(key, new Text(entry.getKey() + "#" + entry.getValue()));
			}
		}
	}

	public static class RelationReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<String, Double>();
			double sum = 0;
			for (Text val : values) {
				String[] res = val.toString().split("#");
				sum += Double.parseDouble(res[1]);
				if (map.containsKey(res[0])) {
					map.put(res[0], map.get(res[0]) + Double.parseDouble(res[1]));
				} else {
					map.put(res[0], Double.parseDouble(res[1]));
				}
			}
			StringBuilder sb = new StringBuilder();
			for (Entry<String, Double> entry : map.entrySet()) {
				String fre = String.format("%.4f", entry.getValue() / sum);
				sb.append(entry.getKey() + "," + fre + "#");
			}
			context.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "RelationGraph");
		// 设置renwu文档作为当前作业的缓存文件
		// job.addCacheFile(new URI("hdfs://master01:54310/user/2014st08/stop-words.txt"));
		job.addCacheFile(new Path("nameDictionary").toUri());
		job.setJarByClass(RelationGraph.class);
		job.setMapperClass(RelationMapper.class);
		job.setCombinerClass(RelationCombiner.class);
		job.setReducerClass(RelationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
