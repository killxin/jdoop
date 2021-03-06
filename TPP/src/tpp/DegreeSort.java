package tpp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class DegreeSort {

	private static HashMap<String, Integer> nodeDeg = new HashMap<String, Integer>();

	public static void degreesort(String input, String output) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line;
		while ((line = br.readLine()) != null) {
			String[] ids = line.split(" ");
			String a = ids[0];
			String b = ids[1];
			if (nodeDeg.containsKey(a)) {
				nodeDeg.put(a, nodeDeg.get(a) + 1);
			} else {
				nodeDeg.put(a, 1);
			}
			if (a.compareTo(b) != 0) {
				if (nodeDeg.containsKey(b)) {
					nodeDeg.put(b, nodeDeg.get(b) + 1);
				} else {
					nodeDeg.put(b, 1);
				}
			}
		}
		br.close();
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		for (Entry<String, Integer> entry : nodeDeg.entrySet()) {
			bw.write(entry.getKey() + "#" + entry.getValue() + "\n");
		}
		bw.close();
	}

	public static void degree_to_hdfs(String input, String output) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(input), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(input));
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);
		while (readLen != -1) {
			System.out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		hdfsInStream.close();
		fs.close();
	}
	public void WriteFile(String hdfs) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
		FSDataOutputStream hdfsOutStream = fs.create(new Path(hdfs));
		hdfsOutStream.write(0);
		hdfsOutStream.close();
		fs.close();
	}

	public void ReadFile(String hdfs) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));

		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);
		while (readLen != -1) {
			System.out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		hdfsInStream.close();
		fs.close();
	}

	public static void test(String input) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line;
		while ((line = br.readLine()) != null) {
			String[] strs = line.split(" ");
			if (strs.length != 2) {
				System.out.println("illegal input");
			}
			if (strs[0].compareTo(strs[1]) != 0) {
				int d0 = 0, d1 = 0;
				if (nodeDeg.containsKey(strs[0])) {
					d0 = nodeDeg.get(strs[0]);
				}
				if (nodeDeg.containsKey(strs[1])) {
					d1 = nodeDeg.get(strs[1]);
				}
				String s0 = d0 + "*" + strs[0];
				String s1 = d1 + "*" + strs[1];
				if (d0 > d1) {
					System.out.println(s1 + "\t" + s0);
				} else {
					System.out.println(s0 + "\t" + s1);
				}
				while (true)
					;
			}
		}
		br.close();
	}

	public static void main(String[] args) throws IOException {
		// degreesort("/home/jdoop/bigdata/twitter_data.txt",
		// "/home/jdoop/bigdata/twitter_degree.txt");
		degreesort(args[0], args[1]);
	}
}
