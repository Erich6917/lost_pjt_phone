package org.phone_producer.producer;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProductLog {

	String startTime = "2019-01-01";
	String endTime = "2019-05-01";

	// 存放tel的list集合
	private List<String> phoneList = new ArrayList<String>();
	// 存放tel和Name的Map集合
	private Map<String, String> phoneNameMap = new HashMap<String, String>();

	/**
	 * 初始化数据
	 */
	public void initPhone() {
		phoneList.add("15369468720");
		phoneList.add("19920860202");
		phoneList.add("18411925860");
		phoneList.add("14473548449");
		phoneList.add("18749966182");
		phoneList.add("19379884788");
		phoneList.add("19335715448");
		phoneList.add("18503558939");
		phoneList.add("13407209608");
		phoneList.add("15596505995");
		phoneList.add("17519874292");
		phoneList.add("15178485516");
		phoneList.add("19877232369");
		phoneList.add("18706287692");
		phoneList.add("18944239644");
		phoneList.add("17325302007");
		phoneList.add("18839074540");
		phoneList.add("19879419704");
		phoneList.add("16480981069");
		phoneList.add("18674257265");
		phoneList.add("18302820904");
		phoneList.add("15133295266");
		phoneList.add("17868457605");
		phoneList.add("15490732767");
		phoneList.add("15064972307");

		phoneNameMap.put("15369468720", "李雁");
		phoneNameMap.put("19920860202", "卫艺");
		phoneNameMap.put("18411925860", "仰莉");
		phoneNameMap.put("14473548449", "陶欣悦");
		phoneNameMap.put("18749966182", "施梅梅");
		phoneNameMap.put("19379884788", "金虹霖");
		phoneNameMap.put("19335715448", "魏明艳");
		phoneNameMap.put("18503558939", "华贞");
		phoneNameMap.put("13407209608", "华啟倩");
		phoneNameMap.put("15596505995", "仲采绿");
		phoneNameMap.put("17519874292", "卫丹");
		phoneNameMap.put("15178485516", "戚丽红");
		phoneNameMap.put("19877232369", "何翠柔");
		phoneNameMap.put("18706287692", "钱溶艳");
		phoneNameMap.put("18944239644", "钱琳");
		phoneNameMap.put("17325302007", "缪静欣");
		phoneNameMap.put("18839074540", "焦秋菊");
		phoneNameMap.put("19879419704", "吕访琴");
		phoneNameMap.put("16480981069", "沈丹");
		phoneNameMap.put("18674257265", "褚美丽");
		phoneNameMap.put("18302820904", "孙怡");
		phoneNameMap.put("15133295266", "许婵");
		phoneNameMap.put("17868457605", "曹红恋");
		phoneNameMap.put("15490732767", "吕柔");
		phoneNameMap.put("15064972307", "冯怜云");
	}

	/**
	 * 产生数据 格式：caller,callee,buildTime,duration
	 *
	 * @return
	 */
	public String product() {
		// ctrl + d 复制此行 ctrl + x 剪切（删除）此行
		// 主叫
		String caller = null;
		String callerName = null;
		// 被叫
		String callee = null;
		String calleeName = null;

		// ctrl + alt+ v 推导出前边 Home End
		int callerIndex = (int) (Math.random() * phoneList.size());
		caller = phoneList.get(callerIndex);
		callerName = phoneNameMap.get(caller);
		while (true) {
			// ctrl + shilt + 下
			int calleeIndex = (int) (Math.random() * phoneList.size());
			callee = phoneList.get(calleeIndex);
			calleeName = phoneNameMap.get(callee);
			if (!caller.equals(callee))
				break;
		}

		// 第三个字段
		String buildTime = randomBuildTime(startTime, endTime);

		// 第四个字段
		DecimalFormat df = new DecimalFormat("0000");
		String duration = df.format((int) (30 * 60 * Math.random()));

		StringBuilder sb = new StringBuilder();
		sb.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);
		return sb.toString();
	}

	/**
	 * 注：传入时间要在时间[startTime,endTime] 公式：起始时间 + （结束时间 - 起始时间）* Math.random()
	 *
	 * @param startTime
	 * @param endTime
	 */
	private String randomBuildTime(String startTime, String endTime) {
		try {
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
			Date startDate = sdf1.parse(startTime);
			Date endDate = sdf1.parse(endTime);

			if (endDate.getTime() <= startDate.getTime()) {
				return null;
			}

			// 公式：起始时间 + （结束时间 - 起始时间）* Math.random()
			long randomTs = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());
			Date resultDate = new Date(randomTs);
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String resultTimeString = sdf2.format(resultDate);

			return resultTimeString;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 把数据写到文件当中
	 *
	 * @param filePath
	 */
	public void writeLog(String filePath) {
		try {
			OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8");
			int maxSize = 1000;
			while (true) {
				Thread.sleep(200);
				String log = product();
				System.out.println(log);
				osw.write(log + "\n");
				osw.flush();

				maxSize -= 1;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
//		args = new String[] { "D:\\calllog.csv" };
		args = new String[] { "calllog.csv" };
		if (args == null || args.length <= 0) {
			System.out.println("没写路径");
			return;
		}
		ProductLog productLog = new ProductLog();
		productLog.initPhone();
		productLog.writeLog(args[0]);
	}

}
