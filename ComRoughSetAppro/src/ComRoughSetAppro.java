import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.LineReader;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ComRoughSetAppro {

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
	 * 根据文件9内容，获取等价分类
	 * 
	 * @param content 文件的内容，行与行之间用“#”分开，如x1 0 0 0 0#x2 1 1 1 1
	 * 
	 * @return Map(key,value) 
	 * 	key:属性类，如0,0,0
	 *  value:对象，“#”分开，如x1#x2
	 */
	public static Map<String, StringBuilder> getMapSortByProperty(String content) {
		Map<String, StringBuilder> sorts = new TreeMap<String, StringBuilder>(
				String_COMPARATOR);
		String[] records = content.split("#");
		int LEN = 0;// 一行记录的长度，x1 0 0 0 1的长度为5
		for (int i = 0; i < records.length; i++) {
			StringTokenizer itr = new StringTokenizer(records[i]);
			if (0 == LEN) {
				// 获取记录的长度
				while (itr.hasMoreTokens()) {
					LEN++;
					itr.nextToken();
				}
				// 重新赋予第一条记录
				itr = new StringTokenizer(records[i]);
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
	 * 根据文件内容，获取根据决策分类
	 * 
	 * @param content 文件的内容，行与行之间用“#”分开，如x1 0 0 0 0#x2 1 1 1 1
	 * 
	 * @return Map(key,value)
	 *  key:决策，如0
	 *  value:对象,"#"分开，如x1#x2
	 */
	public static Map<String, StringBuilder> getMapSortByDecision(String content) {
		Map<String, StringBuilder> sorts = new TreeMap<String, StringBuilder>(
				String_COMPARATOR);
		String[] records = content.split("#");
		int LEN = 0;// 一行记录的长度，x1 0 0 0 1的长度为5
		for (int i = 0; i < records.length; i++) {
			StringTokenizer itr = new StringTokenizer(records[i]);
			if (LEN == 0) {
				// 获取对象的长度
				while (itr.hasMoreTokens()) {
					LEN++;
					itr.nextToken();
				}
				// 重新赋予第一条记录
				itr = new StringTokenizer(records[i]);
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

	 

	public static class calMapper extends Mapper<Text, Text, Text, Text> {

		private final static Text outKey = new Text("");
		private Text outValue = new Text();
		LineReader in;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context) 输出： 等价类及其对应的对象 下近似类 上近似类
		 * 1. 读取文件 
		 * 2. 生成等价类 
		 * 3. 遍历决策类 
		 * 4. 输出
		 */
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// 获取文件内容
			if (0 == value.getLength())
				return;
			int i, j, k;
			ArrayList<String> records=
			new ArrayList(Arrays.asList(value.toString().split("#")));
			Map<String, StringBuilder> propertySorts = getMapSort(
					records, "property");// 等价类
			Map<String, StringBuilder> decisionSorts = getMapSort(
					records, "decision");// 决策类
		 
			// 合并等价类
			StringBuilder property = getEqualPriceMap(propertySorts);

			// 遍历决策类
			for (String decisionKey:decisionSorts.keySet()) {
				String[] decisionIDs = decisionSorts.get(decisionKey)
						.toString().split("#");// 决策ID
				Set decisionIDsSet = new HashSet();// 该决策类对象的集合
				for (i = 0; i < decisionIDs.length; i++)
					decisionIDsSet.add(decisionIDs[i]);

				StringBuilder xiajinsi = new StringBuilder("");// 下近似
				StringBuilder shangjinsi = new StringBuilder("");// 上近似
				Iterator<String> itrPropertyKey = propertySorts.keySet()
						.iterator();
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
				context.write(new Text(decisionKey), new Text(property + "\n"
						+ xiajinsi + "\n" + shangjinsi));
			}
		}
	}

	/*
	 * 根据决策类在各个数据分片中对应的下近似和上近似，得到该决策类真正的下近似和上近似
	 *  step1:分离数据
	 *  step2:计算下近似类、上近似类，合并等价类
	 *  step3:获取下近似对象 step4:获取上近似对象
	 * 
	 *  @param key: 决策类
	 * 
	 *  @param value:
	 * 	0,0,0~~x1~~~0,1,0~~x4~~~0,1,1~~x7~~~1,0,1~~x5~~~1,1,1~~x2#x3#x6//等价类
	 * 	0,1,0#0,1,1#1,0,1//下近似类
	 *  0,1,0#0,1,1#1,0,1#1,1,1//上近似类
	 */
	public static class calReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder outValue = new StringBuilder("");
			Set acceptXiajinsiLeiSet = new HashSet();// 接受的下近似类集合
			Set rejectXiajinsiLeiSet = new HashSet();// 排除的下近似类集合
			Set shangjinsiLeiSet = new HashSet();// 上近似类集合
			Map<String, StringBuilder> propertySorts = new TreeMap<String, StringBuilder>(
					String_COMPARATOR);// 所有分片合并后的等价类
			int splitNum = 0;// 分片的数目
			StructReduceValue first = null;// 一个分片对应的记录
			StructReduceValue last = null;
			// 将分片数据分开
			for (Text t : values) {
				StructReduceValue p = new StructReduceValue(t.toString());
				if (null == first) {
					first = p;
					last = p;
				} else {
					last.pNext = p;
					last = p;
				}
				splitNum++;
			}
			for (StructReduceValue p = first; null != p; p = p.pNext) {

				// 遍历下近似类
				Iterator pItr = p.xiajinsiLeiSet.iterator();
				while (pItr.hasNext()) {
					String sort = pItr.next().toString();
					// 已经被添加或者被排除的话，则继续
					if (acceptXiajinsiLeiSet.contains(sort)
							|| rejectXiajinsiLeiSet.contains(sort))
						continue;
					boolean reject = false;
					for (StructReduceValue p1 = first; null != p1; p1 = p1.pNext) {
						if (p1.propertyLeiSet.contains(sort)) {
							if (!p1.xiajinsiLeiSet.contains(sort)) {
								reject = true;
								break;
							}
						}
					}
					if (reject)
						rejectXiajinsiLeiSet.add(sort);
					else
						acceptXiajinsiLeiSet.add(sort);

				}
				// 上近似类
				shangjinsiLeiSet.addAll(p.shangjinsiLeiSet);

				// 合并等价类
				Iterator<String> keys = p.propertySorts.keySet().iterator();
				while (keys.hasNext()) {
					String sort = keys.next();
					StringBuilder pID = new StringBuilder(
							p.propertySorts.get(sort));
					// 存在，则合并
					if (propertySorts.containsKey(sort)) {
						StringBuilder IDs = propertySorts.get(sort);
						IDs.append("#" + pID);
						propertySorts.remove(sort);
						propertySorts.put(sort, IDs);
					}
					// 不存在则放进去
					else {
						propertySorts.put(sort, pID);
					}
				}

			}

			// 合并下近似对象
			StringBuilder xiajinsi = new StringBuilder("");
			Iterator<String> x = acceptXiajinsiLeiSet.iterator();
			while (x.hasNext()) {
				StringBuilder ids = propertySorts.get(x.next());
				if (0 == xiajinsi.length()) {
					xiajinsi.append(ids);
				} else {
					xiajinsi.append("#" + ids);
				}
			}
			// 合并上近似对象
			StringBuilder shangjinsi = new StringBuilder("");
			Iterator<String> s = shangjinsiLeiSet.iterator();
			while (s.hasNext()) {
				StringBuilder ids = propertySorts.get(s.next());
				if (0 == shangjinsi.length()) {
					shangjinsi.append(ids);
				} else {
					shangjinsi.append("#" + ids);
				}
			}

			outValue.append("下近似类：" + xiajinsi);
			outValue.append("\n上近似类：" + shangjinsi);
			// 获取上近似类对应的对象

			context.write(key, new Text(outValue.toString()));
		}
	}

	public static void deletFile(Path path, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path);

	}
 
	
	public static class KeyComparator extends WritableComparator {
	    protected KeyComparator() {
	        super(Text.class, true);
	    }

	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
 
	        String[]key1=((Text)w1).toString().split(" ");
	        String[]key2=((Text)w2).toString().split(" ");
	        //int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
	        for(int i=0;i<key1.length;i++){
	        	int i1=Integer.parseInt(key1[i]);
	        	int i2=Integer.parseInt(key2[i]);
	        	if(i1-i2<0)
	        		return -1;
	        	if(i1-i2>0)
	        		return 1;
	        }
	       return 0;
	    }
	}

	/**
	 * @param args
	 *  args[0] 输入路径
	 *  args[1] 输出路径
	 *  args[2] 采样数目
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		// System.out.println("1212".split("#")+"********");
		Configuration conf = new Configuration();

		Job job = new Job(conf, "ComRoughSetAppro");
		job.setJarByClass(ComRoughSetAppro.class);
		job.setMapperClass(calMapper.class);

		job.setReducerClass(calReduce.class);

		// 采样的records数目
		conf.setInt("NumRecords", 1000);
		int numReduce = 6;
		for (int i = 0; i < args.length; i++) {
			System.out.println("*" + args[i]);
			if (args[i].equals("-r")) {
				numReduce = Integer.parseInt(args[i + 1]);
				// printf()
				break;
			}
		}
		job.setNumReduceTasks(numReduce);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(
				args[0]));
		deletFile(new Path(args[1]), conf);
		FileOutputFormat.setOutputPath(job, new Path(
				args[1]));

		// 采集args[2]条记录,并生成分区文件
		job.setSortComparatorClass(KeyComparator.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		conf.set("mapreduce.totalorderpartitioner.path", args[0]+"__partition.lst");
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.SplitSampler<Text, Text>(
				Integer.parseInt(args[2]));
		InputSampler.writePartitionFile(job, sampler);

		job.waitForCompletion(true);
	}

}