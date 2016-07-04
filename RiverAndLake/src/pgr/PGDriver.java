package pgr;

public class PGDriver {
	private static int times = 5; // 设置迭代次数

	public static void main(String[] args) throws Exception {
		String[] forInit = { "", args[1] + "/Data0" };
		forInit[0] = args[0];
		PGInit.main(forInit);
		String[] forItr = { "", "" };
		for (int i = 0; i < times; i++) {
			forItr[0] = args[1] + "/Data" + i;
			forItr[1] = args[1] + "/Data" + String.valueOf(i + 1);
			PGIter.main(forItr);
		}
	}
}
