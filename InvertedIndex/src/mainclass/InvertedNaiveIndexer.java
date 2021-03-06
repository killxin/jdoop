package mainclass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedNaiveIndexer {
	/** 自定义FileInputFormat **/
	public static class FileNameInputFormat extends FileInputFormat<Text, Text> {
		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileNameRecordReader fnrr = new FileNameRecordReader();
			fnrr.initialize(split, context);
			return fnrr;
		}
	}

	/** 自定义RecordReader **/
	public static class FileNameRecordReader extends RecordReader<Text, Text> {
		String fileName;
		
		//通过聚合实现代码复用
		LineRecordReader lrr = new LineRecordReader();

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return new Text(fileName);
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return lrr.getCurrentValue();
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
			lrr.initialize(arg0, arg1);
			//去掉文件名后缀
			StringBuilder sb = new StringBuilder();
			String[] str = ((FileSplit) arg0).getPath().getName().split("\\.");
			for(String s: str){
				if(s.toLowerCase().compareTo("txt")==0){
					break;
				}
				sb.append(s);
			}
			fileName = sb.toString();
			
		}

		public void close() throws IOException {
			lrr.close();
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lrr.nextKeyValue();
		}

		public float getProgress() throws IOException, InterruptedException {
			return lrr.getProgress();
		}
	}

	public static class InvertedNaiveIndexMapper extends Mapper<Text, Text, Text, Text> {
		private String pattern = "[\\w]"; // 正则表达式，代表0-9, a-z, A-Z,的所有字

		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// map()函数这里使用自定义的FileNameRecordReader
			// 得到key: filename文件名; value: line_string每一行的内容
			String temp = new String();
			String line = value.toString().toLowerCase();
			line = line.replaceAll(pattern, " "); // 将0-9, a-z, A-Z的字符替换为空格
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				temp = itr.nextToken();
			//	if (!stopwords.contains(temp)) {
					Text word = new Text();
					word.set(temp + "#" + key);
					context.write(word, new Text("1"));
			//	}
			}
		}
	}

	/** 使用Combiner将Mapper的输出结果中value部分的词频进行统计 **/
	public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			String term = key.toString().split("#")[0]; // <term#docid>=>term
			String docid = key.toString().split("#")[1];
			result.set(docid+"#"+sum);
			//违反原则
			context.write(new Text(term), result);
		}
	}
	
	public static class InvertedNaiveIndexReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String,Integer> hm = new HashMap<String,Integer>();
			for(Text val : values){
				String docid = val.toString().split("#")[0];
				int frequent = Integer.parseInt(val.toString().split("#")[1]);
				if(hm.containsKey(docid)){
					hm.put(docid, hm.get(docid)+frequent);
				}
				else{
					hm.put(docid, frequent);
				}
			}
			StringBuilder out = new StringBuilder();
			float count = 0;
			float docs = 0;
			for(Map.Entry<String, Integer> entry : hm.entrySet()){
				out.append(entry.getKey()+":"+entry.getValue()+";");
				count+=entry.getValue();
				docs++;
			}
			if (count > 0){
				String frequent = String.format("%.2f",count/docs);
				context.write(key, new Text(frequent+","+out.toString()));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		DistributedCache.addCacheFile(new URI("hdfs://master01:54310/user/2014st08/stop-words.txt"), conf);// 设置停词列表文档作为当前作业的缓存文件
		Job job = new Job(conf, "inverted naive index");
		job.setJarByClass(InvertedIndexer.class);
		job.setInputFormatClass(FileNameInputFormat.class);
		job.setMapperClass(InvertedNaiveIndexMapper.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(InvertedNaiveIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
