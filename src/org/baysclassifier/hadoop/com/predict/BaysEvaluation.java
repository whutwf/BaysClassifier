package org.baysclassifier.hadoop.com.predict;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.baysclassifier.hadoop.com.help.MyUnits;

public class BaysEvaluation {
	private static String[] country = {"CHINA", "UK"};
	private static int TP[] = {0, 0};
	private static int FP[] = {0, 0};
	private static int FN[] = {0, 0};
	private static int TN[] = {0, 0};
	public static void run(Configuration conf) throws IllegalArgumentException, IOException {
		//读取评估类别，计算P R F1
		FileSystem fSystem = FileSystem.get(URI.create(MyUnits.PREDICT_RESULT.concat("/part-r-00000")), conf);
		FSDataInputStream inputStream = fSystem.open(new Path(MyUnits.PREDICT_RESULT.concat("/part-r-00000")));
		BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
		String strLine = "";
		while((strLine = buffer.readLine()) != null) {
			for(int i = 0; i < country.length; i++) {
				String[] temp = strLine.split("\t");
				if(country[i].equals(temp[0])){
					if(temp[2].equals(temp[0])){
						TP[i]++;
					}else{
						FN[i]++;
					}
				}else{
					if(country[i].equals(temp[2])){
						FP[i]++;
					}else{
						TN[i]++;
					}
				}
			}
		}
		
		//保存评估结果到文件中，
		Path outputPath = new Path(MyUnits.EVALUATION_RESULT);
		FileSystem outFs = outputPath.getFileSystem(conf);
		FSDataOutputStream outputStream = outFs.create(outputPath);
		String ctx ="";
		double P[] = { 0.0, 0.0};
		double R[] = { 0.0, 0.0};
		double F1[] = { 0.0, 0.0};
		for (int i = 0; i < 2; i++) {
			P[i] = (double) TP[i] / (TP[i] + FP[i]);
			R[i] = (double) TP[i] / (TP[i] + FN[i]);
			F1[i] = (double) 2 * P[i] * R[i] / (P[i] + R[i]);
			ctx += country[i] + "\tP=" + P[i] +"\tR=" + R[i] + "\tF1=" + F1[i] + "\n";
			System.out.println(country[i] + "\tP=" + P[i] +"\tR=" + R[i] + "\tF1=" + F1[i]);
		}
		outputStream.writeBytes(ctx);
		outputStream.close();
	}
}
