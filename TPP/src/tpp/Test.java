package tpp;

public class Test {
	public static void main(String[] args) {
		for (int i = 0; i < 20; i++) {
			String str = String.valueOf(i);
			System.out.println(str+"#"+str.hashCode()+","+str.hashCode()%4);
		}
	}

}