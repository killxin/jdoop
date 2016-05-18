package triangle;

public class Driver {
	public static void main(String[] args) throws Exception {
		String[] forGB = { args[0], args[1] + "/GraphSchema" };
		GraphBuilder.main(forGB);
		String[] forTS = { args[1] + "/Data0", args[1] + "/TriangleNum" };
		TriangleSearch.main(forTS);
	}
}