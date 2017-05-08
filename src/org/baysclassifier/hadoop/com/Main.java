package org.baysclassifier.hadoop.com;

import org.apache.hadoop.conf.Configuration;
import org.baysclassifier.hadoop.com.predict.BaysEvaluation;
import org.baysclassifier.hadoop.com.predict.BaysPredict;
import org.baysclassifier.hadoop.com.predict.BaysProbility;
import org.baysclassifier.hadoop.com.train.countdoc.DocCount;
import org.baysclassifier.hadoop.com.train.countterm.TermCount;

public class Main {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		
		DocCount.run(conf);
		TermCount.run(conf);
		
		BaysProbility.run(conf);
		System.out.println(BaysProbility.ProbMap.getNotFoundMap());
		BaysPredict.run(conf);
		BaysEvaluation.run(conf);
	}
}
