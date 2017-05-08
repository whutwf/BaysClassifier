package org.baysclassifier.hadoop.com.help;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyUnits {	
	//训练文档输入位置
	public final static String INPUT_TRAIN_PATH = "hdfs://localhost:9000/user/hadoop/country/train";
	//测试文档输入位置
	public final static String INPUT_TEST_PATH = "hdfs://localhost:9000/user/hadoop/country/test";
	//统计类别文件个数存储位置
	public final static String OUTPUT_FILES_IN_CLASS = "hdfs://localhost:9000/user/hadoop/output/_filesInClass/filesInClass-r-00000";
	//统计类别每个单词个数存储位置
	public final static String OUTPUT_WORDS_IN_CLASS = "hdfs://localhost:9000/user/hadoop/output/_wordsInClass/wordsInClass-r-00000";
	//先验概率存储位置, 没有存储，直接使用静态变量
	public final static String PRIOR_PROBALITY = "hdfs://localhost:9000/user/hadoop/output/probility/priorProbability.txt";
	//未出现单词概率，静态访问，没有存储到文件
	public final static String NOT_EXIT_FOUND_PRIORP = "hdfs://localhost:9000/user/hadoop/output/probility/_notFound/notFound-m-00000";
	//单词条件概率存储位置，直接使用静态变量访问
	public final static String CONDITION_PROBILITY = "hdfs://localhost:9000/user/hadoop/output/probility/_condition/condtion-m-00000";
	//测试结果存储位置
	public final static String PREDICT_RESULT = "hdfs://localhost:9000/user/hadoop/output/result/_predict/predict-m-00000";
	//测试结果存储位置
	public final static String EVALUATION_RESULT = "hdfs://localhost:9000/user/hadoop/output/result/_evaluation/evaluation-m-00000";
	// 判断一个字符串是否含有数字
	public static boolean hasDigit(String content) {
		boolean flag = false;  
		Pattern pattern = Pattern.compile(".*\\d+.*");
		Matcher matcher =pattern.matcher(content);
		if(matcher.matches()){
			flag = true;
		}
		return flag;
	}
}
