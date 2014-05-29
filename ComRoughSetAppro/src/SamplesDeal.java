import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

public class SamplesDeal {
	TreeMap<Text, Integer> result =null;

	/*
	 * 根据文件的内容，获取等价分类
	 * @input List<String> records
	 * 	record:x1 0 0 0 1 
 	 * 
	 * @return Map<key,value>
	 *  key:属性类，如0,0,0
	 *  value:对象，“#”分开，如x1#x2
	 */
	public static Map<String, StringBuilder> getMapSortByProperty(
			List<String> records) {
		Map<String, StringBuilder> sorts = new TreeMap<String, StringBuilder>(
				String_COMPARATOR);
		int LEN = 0;// 一行记录的长度，x1 0 0 0 1的长度为5
		for (String record : records) {
			StringTokenizer itr = new StringTokenizer(record);
			if (0 == LEN) {
				// 获取记录的长度
				while (itr.hasMoreTokens()) {
					LEN++;
					itr.nextToken();
				}
				// 重新赋予第一条记录
				itr = new StringTokenizer(record);
			}
			String sort = "";
			StringBuilder ID = new StringBuilder(itr.nextToken());// 获取该条记录中的对象

			// 属性
			for (int j = 0; j < LEN - 3; j++)
				sort += itr.nextToken() + ",";
			sort += itr.nextToken();

			if (null != sorts.get(sort)) {
				// 已有该等价类，添加到后面的对象当中
				StringBuilder IDs = sorts.get(sort);
				IDs.append("#" + ID);
				sorts.remove(sort);
				sorts.put(sort, IDs);
			} else {
				// 第一个等价类
				sorts.put(sort, ID);
			}
		}
		return sorts;
	}

	/*
	 * 根据文件的内容，获取根据决策分类
	 * @input List<String> records
	 * 	record:x1 0 0 0 1 
	 * 
	 * @return Map(key,value) 
	 * key:决策，如1 
	 * value:对象,"#"分开，如x1#x2
	 */
	public static Map<String, StringBuilder> getMapSortByDecision(List<String> records) {
		Map<String, StringBuilder> sorts = new TreeMap<String, StringBuilder>(
				String_COMPARATOR);
 		int LEN = 0;// 一行记录的长度，x1 0 0 0 1的长度为5
		for (String record:records) {
			StringTokenizer itr = new StringTokenizer(record);
			if (LEN == 0) {
				// 获取对象的长度
				while (itr.hasMoreTokens()) {
					LEN++;
					itr.nextToken();
				}
				// 重新赋予第一条记录
				itr = new StringTokenizer(record);
			}
			String sort = "";
			StringBuilder ID = new StringBuilder(itr.nextToken());
			for (int j = 0; j < LEN - 2; j++)
				itr.nextToken();
			sort = itr.nextToken();

			if (null != sorts.get(sort)) {
				// 已经包含该决策类
				StringBuilder IDs = sorts.get(sort);
				IDs.append("#" + ID);
				sorts.remove(sort);
				sorts.put(sort, IDs);
			} else {
				// 第一个决策类
				sorts.put(sort, ID);
			}
		}
		return sorts;
	}

	/*
	 * 根据决策或者属性对文档进行分类
	 * 
	 * @param method 分类的方法
	 * 
	 * @return Map(key,value) key:决策或属性，如property或decision value:对象，如x1#x2
	 */
	public static Map<String, StringBuilder> getMapSort(List<String> records,
			String method) {
		if (method == "property")
			return getMapSortByProperty(records);
		else
			return getMapSortByDecision(records);

	}

	static final Comparator<String> String_COMPARATOR = new Comparator<String>() {
		/*
		 * 直接用字符串默认的大小比较，代替该优先级 若o1的优先级高于o2，则返回负数
		 */
		public int compare(String o1, String o2) {
			return o1.compareTo(o2);
		}
	};

	/*
	 * 将等价类合并成字符串
	 * 
	 * @param propertySorts为等价类
	 * 
	 * @return 等价类的字符串表示 类间用“~~~”隔开，类和对象间用“~~”隔开，对象间用"#"隔开
	 * 如：0,0,0~~x8#x9~~~0,0,1~~x12~~~0,1,1~~x14~~~1,0,1~~x10~~~1,1,0~~x11#x13
	 */
	public static StringBuilder getEqualPriceMap(
			Map<String, StringBuilder> propertySorts) {
		StringBuilder property = new StringBuilder("");
		Iterator<String> itr = propertySorts.keySet().iterator();
		while (itr.hasNext()) {
			String sort = itr.next();
			StringBuilder IDs = propertySorts.get(sort);
			if (0 == property.length()) {
				// 第一条等价类
				property.append(sort + "~~" + IDs);
			} else {

				property.append("~~~" + sort + "~~" + IDs);
			}

		}
		return property;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * 输出：TreeMap<Text,Integer>
	 * key:决策类
	 * value:该决策类的大小
	 */
	public TreeMap<Text, Integer> map(List<String> records,RawComparator comparator) throws IOException,
			InterruptedException {
		result=new TreeMap<Text, Integer>(comparator);
		// 获取文件内容
		if (0 == records.size())
			return null;
		int i, j, k;
		Map<String, StringBuilder> propertySorts = getMapSort(records, "property");// 等价类
		Map<String, StringBuilder> decisionSorts = getMapSort(records, "decision");// 决策类
 
		// 合并等价类
		StringBuilder property = getEqualPriceMap(propertySorts);

		// 遍历决策类
		for (String decisionKey:decisionSorts.keySet()) {
 			String[] decisionIDs = decisionSorts.get(decisionKey).toString()
					.split("#");// 决策ID
			Set decisionIDsSet = new HashSet();// 该决策类对象的集合
			for (i = 0; i < decisionIDs.length; i++)
				decisionIDsSet.add(decisionIDs[i]);

			StringBuilder xiajinsi = new StringBuilder("");// 下近似
			StringBuilder shangjinsi = new StringBuilder("");// 上近似
			Iterator<String> itrPropertyKey = propertySorts.keySet().iterator();
			// 遍历等价类
			while (itrPropertyKey.hasNext()) {
				String propertyKey = itrPropertyKey.next();
				String[] propertyIDs = propertySorts.get(propertyKey)
						.toString().split("#");// 条件ID

				Set propertyIDsSet = new HashSet();
				for (i = 0; i < propertyIDs.length; i++)
					propertyIDsSet.add(propertyIDs[i]);

				// 等价类对象集合包含于决策类对象集合，为下近似
				if (decisionIDsSet.containsAll(propertyIDsSet)) {
					if (0 == xiajinsi.length())
						xiajinsi.append(propertyKey);
					else
						xiajinsi.append("#" + propertyKey);
				}
				// 等价类对象集合与决策类对象集合相交，为上近似
				for (i = 0; i < propertyIDs.length; i++) {
					if (decisionIDsSet.contains(propertyIDs[i])) {
						if (0 == shangjinsi.length())
							shangjinsi.append(propertyKey);
						else
							shangjinsi.append("#" + propertyKey);
						break;
					}

				}
			}
			if (0 == xiajinsi.length())
				xiajinsi.append("NULL");
			if (0 == shangjinsi.length())
				shangjinsi.append("NULL");

			// different from mapper.map
			Text outKey = new Text(decisionKey);
			Integer v = result.get(outKey);
			if (null == v)
				v = 0;
			v += property.length() + xiajinsi.length() + shangjinsi.length();
			result.put(outKey, v);

		}
		return result;
	}
}
