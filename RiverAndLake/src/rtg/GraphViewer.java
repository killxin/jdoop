package rtg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

class NodePair {
	public int id;
	public int label;
}

public class GraphViewer {

	public static String pgrPath = "/home/jdoop/bigdata/PGROut/Data5/part-r-00000";

	public static String lpaPath = "/home/jdoop/bigdata/LPAOut/Data5/part-r-00000";

	public static String nodeOutPath = "/home/jdoop/bigdata/GraphOut/node.csv";

	public static String edgeOutPath = "/home/jdoop/bigdata/GraphOut/edge.csv";

	public static int temp = 0;

	public static void main(String[] args) throws IOException {
		Map<String, NodePair> nodeMap = new TreeMap<String, NodePair>();
		String line;
		BufferedReader br = new BufferedReader(new FileReader(lpaPath));
		while ((line = br.readLine()) != null) {
			String[] strs = line.split("\t");
			String[] keys = strs[0].split("@");
			int lab = Integer.parseInt(keys[1]);
			NodePair np = new NodePair();
			np.id = temp++;
			np.label = lab;
			nodeMap.put(keys[0], np);
		}
		br.close();
		br = new BufferedReader(new FileReader(pgrPath));
		BufferedWriter nbw = new BufferedWriter(new FileWriter(new File(nodeOutPath)));
		nbw.write("id,label,timeset,modularity_class,weight\n");
		BufferedWriter ebw = new BufferedWriter(new FileWriter(new File(edgeOutPath)));
		ebw.write("Source,Target,Type,id,label,timeset,weight\n");
		while ((line = br.readLine()) != null) {
			String[] strs = line.split("\t");
			String[] keys = strs[0].split("@");
			double pgr = Double.parseDouble(keys[1]);
			NodePair np = nodeMap.get(keys[0]);
			nbw.write(String.format("%d,%s,,%d,%.2f\n", np.id,keys[0],np.label,pgr));
			nodeMap.put(keys[0], np);
			String[] neighs = strs[1].split("#");
			for (String neigh : neighs) {
				String[] vals = neigh.split(",");
				NodePair nnp = nodeMap.get(vals[0]);
				Double w = Double.parseDouble(vals[1]);
				ebw.write(String.format("%d,%d,Undirected,%d,,,%.2f\n",np.id,nnp.id,temp++,w));
			}
		}
		nbw.close();
		ebw.close();
	}
}
