import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class StructReduceValue {
	/*
	 * 存放一个数据分片的等价类、等价类的类集合、下近似类的类集合、上近似类的类集合
	 * 
	 */
	static Comparator<String> String_COMPARATOR = new Comparator<String>() {
		// 若o1的优先级高于o2，则返回负数
		public int compare(String o1, String o2) {
			return o1.compareTo(o2);
		}
	};
	// 存放Reduce值的列表
	Map<String, StringBuilder> propertySorts = new TreeMap<String, StringBuilder>(
			String_COMPARATOR);// 等价类
	Set propertyLeiSet = new HashSet();//等价类的类集合
	Set xiajinsiLeiSet = new HashSet();//下近似类的类集合
	Set shangjinsiLeiSet = new HashSet();//上近似类的类集合
	StructReduceValue pNext = null;

	/*
	 * 
	 * @param value:
	 * 0,0,0~~x1~~~0,1,0~~x4~~~0,1,1~~x7~~~1,0,1~~x5~~~1,1,1~~x2#x3#x6//等价类
	 * 0,1,0#0,1,1#1,0,1//下近似类
	 * 0,1,0#0,1,1#1,0,1#1,1,1//上近似类
	 */
	StructReduceValue(String value) {
		String[] t = value.split("\n");
		// 等价类
		String[] dengjialei = t[0].split("~~~");
		for (int i = 0; i < dengjialei.length; i++) {
			propertyLeiSet.add(dengjialei[i].split("~~")[0]);
			propertySorts.put(dengjialei[i].split("~~")[0], new StringBuilder(
					dengjialei[i].split("~~")[1]));
		}

		// 下近似
		if ("NULL" != t[1]) {
			String[] xiajinsi = t[1].split("#");
			for (int i = 0; i < xiajinsi.length; i++)
				xiajinsiLeiSet.add(xiajinsi[i]);
		}
		// 上近似
		String[] shangjinsi = t[2].split("#");
		for (int i = 0; i < shangjinsi.length; i++)
			shangjinsiLeiSet.add(shangjinsi[i]);

	}

}
