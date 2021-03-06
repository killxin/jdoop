package mainclass;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedHBase {
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

	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, IntWritable> {
		private Set<String> stopwords;
		//private Path[] localFiles;
		private Path localFiles;
		private String pattern = "[\\w]"; // 正则表达式，代表0-9, a-z, A-Z,的所有字

		public void /*unused_*/setup(Context context) throws IOException, InterruptedException {
			stopwords = new TreeSet<String>();
		    URI[] cacheFile = context.getCacheFiles();
	        localFiles = new Path(cacheFile[0]);

	        /*
				String line;
				BufferedReader br = new BufferedReader(new FileReader(localFiles.toString()));
				while ((line = br.readLine()) != null) {
					StringTokenizer itr = new StringTokenizer(line);
					while (itr.hasMoreTokens()) {
						stopwords.add(itr.nextToken());
					}
				}	
			*/
			
	        Scan scan = new Scan();
	        ResultScanner rs = stop_table.getScanner(scan);
	        for(Result result : rs){
	        	byte[] bytes = result.getRow();
	        //	System.out.println(new String(bytes));
	        	stopwords.add(new String(bytes));
	        }
			System.out.println("setup load stop_words finished!");
		}
		
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// map()函数这里使用自定义的FileNameRecordReader
			// 得到key: filename文件名; value: line_string每一行的内容
			String temp = new String();
			String line = value.toString().toLowerCase();
			line = line.replaceAll(pattern, " "); // 将0-9, a-z, A-Z的字符替换为空格
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				temp = itr.nextToken();
				if (!stopwords.contains(temp)) {
					Text word = new Text();
					word.set(temp + "#" + key);
					context.write(word, new IntWritable(1));
				}
		        else{
		      //  	System.out.println(temp+" in stop_words");
		        }
			}
		}
	}

	/** 使用Combiner将Mapper的输出结果中value部分的词频进行统计 **/
	public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer **/
	public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String term = new String();
			term = key.toString().split("#")[0]; // <term#docid>=>term
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
		private Text word1 = new Text();
		private Text word2 = new Text();
		String temp = new String();
		static Text CurrentItem = new Text(" ");
		static List<String> postingList = new ArrayList<String>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			word1.set(key.toString().split("#")[0]);
			temp = key.toString().split("#")[1];
			for (IntWritable val : values) {
				sum += val.get();
			}
			word2.set(temp + ":" + sum);
			if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
				cleanup(context);
				/*
				StringBuilder out = new StringBuilder();
				double count = 0;
				double docs = 0;
				for (String p : postingList) {
					out.append(p);
					out.append(";");
					count = count +Double.parseDouble(p.substring(p.indexOf(":") + 1));
					docs++;
				}
				//out.append("<total," + count + ">.");
				String frequent = String.format("%.2f",count/docs);
				if (count > 0){
					context.write(CurrentItem, new Text(frequent+","+out.toString()));
				}
				*/
				postingList = new ArrayList<String>();
			}
			CurrentItem = new Text(word1);
			postingList.add(word2.toString()); // 不断向postingList也就是文档名称中添加词表
		}

		// cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况

		public void cleanup(Context context) throws IOException, InterruptedException {
			StringBuilder out = new StringBuilder();
			double count = 0;
			double docs = 0;
			for (String p : postingList) {
				out.append(p);
				out.append(";");
				count = count +Double.parseDouble(p.substring(p.indexOf(":") + 1));
				docs++;
			}
			//out.append("<total," + count + ">.");
			String frequent = String.format("%.2f",count/docs);
			//Double frequent_num = count/docs;
			if (count > 0 && CurrentItem.getLength() != 0){
				Put put = new Put(Bytes.toBytes(CurrentItem.toString()));
				put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("frequent"), Bytes.toBytes(frequent));
				table.put(put);
				//frequent_set.put(CurrentItem.toString(),frequent_num);
				//context.write(CurrentItem, new Text(frequent+","+out.toString()));
				context.write(CurrentItem, new Text(out.toString()));
			}
		}

	}

	public static Table table = null;
	public static Table stop_table = null;
	
	public static void main(String[] args) throws Exception {
		/*connect to hbase and create table 'wuxia'*/
		Configuration confi = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(confi);
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("wuxia");
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(tableName + " table already exists!");
		}
		System.out.println(tableName + " table create start!");
		HTableDescriptor tableDec = new HTableDescriptor(tableName);
		tableDec.addFamily(new HColumnDescriptor("family"));
		admin.createTable(tableDec);
		System.out.println(tableName + " table create success!");
		table = connection.getTable(tableName);
		
		tableName = TableName.valueOf("stop_words");
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(tableName + " table already exists!");
		}
		System.out.println(tableName + " table create start!");
		tableDec = new HTableDescriptor(tableName);
		tableDec.addFamily(new HColumnDescriptor("family"));
		admin.createTable(tableDec);
		System.out.println(tableName + " table create success!");
		stop_table = connection.getTable(tableName);
		String line;
		BufferedReader br = new BufferedReader(new FileReader("/home/jdoop/bigdata/stop_words.txt"));
		while ((line = br.readLine()) != null) {
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				Put put = new Put(Bytes.toBytes(itr.nextToken()));
				put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("ignorable"), Bytes.toBytes("ignore"));
				stop_table.put(put);
			}
		}
		System.out.println("load stop_words finished!");
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "inverted index");
		//向分布式缓存中添加文件
		//job.addCacheFile(new Path("stop_words.txt").toUri());
		
		job.setJarByClass(InvertedHBase.class);
		job.setInputFormatClass(FileNameInputFormat.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setPartitionerClass(NewPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
