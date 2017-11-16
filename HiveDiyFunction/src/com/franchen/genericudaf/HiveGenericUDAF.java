package com.franchen.genericudaf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser.mapType_return;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.ResetProcessor;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HiveGenericUDAF extends AbstractGenericUDAFResolver {
	
	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		/**
		 * Note:
		 * 这里的info功能更加全面，可以判断的功能更多。
		 * 例如info.isAllColumns()和info.isDistinct()
		 * 在getEvaluator(TypeInfo[] info)中就没有。
		 * 通过info.getParameters()即可获得重载函数的TypeInfo[] info。
		 * 之所以再实现getEvaluator(TypeInfo[] info)，是为了逻辑更加清楚、简洁。
		 */
		
		if (info.isAllColumns()){
			throw new SemanticException(
				"Do not support all columns (*) !"	
			);
		}
		
		if (info.isDistinct()) {
			throw new SemanticException(
				"Do not support distinct !"	
			);
		}
		
		return getEvaluator(info.getParameters());
	}


	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		
		/**
		 * Note:
		 * 这里实现对参数个数，类型的检查，
		 * 并根据参数类型调用不同的GenericUDAFEvaluator
		 */
		
		// 1. 检查参数个数和类型
		if (info.length != 1){
			throw new UDFArgumentTypeException(
					info.length - 1,
					"Exactly one argument is expected."
			);
		}
		if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,  
			          "Only primitive type arguments are accepted but "  
			          + info[0].getTypeName() + " is passed."); 
		}
		
		// 2. 根据需求返回不同的GenericUDAFEvaluator类
		switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
		case INT:
//			return new IntGenericUDAF();
		case DOUBLE:
//			return new DoubleGenericUDAF();
		case STRING:
			return new StringGenericUDAF();
		default:
			throw new UDFArgumentTypeException(0,  
			          "Only string or double or int type arguments are accepted but "  
			          + info[0].getTypeName() + " is passed.");
		}
	}
	
	public static class StringGenericUDAF extends GenericUDAFEvaluator{
		
		// Input
		// For PARTIAL1 and COMPLETE
		PrimitiveObjectInspector inputOI;
		// For PARTIAL2 and FINAL
		StructObjectInspector soi;
		StructField totalField;
		StructField countField;
		DoubleObjectInspector totalOI;
		IntObjectInspector countOI;
		
		// Output
		// For FINAL and COMPLETE
		Text result;
		// For PARTIAL1 and PARTIAL2
		Object[] partialResult;
		
		/**
		 * 初始化GenericUDAFEvaluator
		 */
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			// 1. 宣告参数只有一个，否则报错
			assert (parameters.length == 1);
			// 2. 初始化父类
			super.init(m, parameters);
			
			// 3. 设置输入类型
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				// 第二节说过，使用Text作为IntWritable和DoubleWritable的中间层
				inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			} else {
				soi = (StructObjectInspector) parameters[0];
				totalField = soi.getStructFieldRef("total");
				countField = soi.getStructFieldRef("count");
				totalOI = (DoubleObjectInspector) totalField.getFieldObjectInspector();
				countOI = (IntObjectInspector) countField.getFieldObjectInspector();
			}
			
			// 4. 设置返回类型
			if (m == Mode.FINAL || m == Mode.COMPLETE) {
				result = new Text();
				return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			} else {
				partialResult = new Object[2];
				partialResult[0] = new DoubleWritable(0);
				partialResult[1] = new IntWritable(0);
				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
				foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
				ArrayList<String> fieldName = new ArrayList<String>();
				fieldName.add("total");
				fieldName.add("count");
				return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, foi);
			}
			
		}
		
		// 继承AggregationBuffer类实现数据对象
		static class StudentScore implements AggregationBuffer{
			double scoreTotal;
			int scoreCount;
			public StudentScore(){
				reset();
			}
			public void reset(){
				this.scoreCount = 0;
				this.scoreTotal = 0;
			}
			public void addScore(double score) {
				this.scoreTotal += score;
				this.scoreCount ++;
			}
			public String getResult(){
				return "This student took " + this.scoreCount + " subject"
						+ ", total score is " + this.scoreTotal
						+ ", average score is " + (this.scoreTotal / this.scoreCount) + ".";
			}
		}

		/**
		 * 返回新的AggregationBuffer对象
		 */
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			// TODO 自动生成的方法存根
			return new StudentScore();
		}
		
		/**
		 * 复用AggregationBuffer对象
		 */
		@Override
		public void reset(AggregationBuffer arg0) throws HiveException {
			StudentScore studentScore = (StudentScore) arg0;
			studentScore.reset();
		}

		/**
		 * 处理一行数据
		 */
		@Override
		public void iterate(AggregationBuffer arg0, Object[] arg1)
				throws HiveException {
			assert(arg1.length == 1);
			// 处理数据
			if (null != arg1 && null != arg1[0]) {
				double score = PrimitiveObjectInspectorUtils.getDouble(arg1[0], inputOI);
				StudentScore studentScore = (StudentScore) arg0;
				studentScore.addScore(score);
			}
		}
		
		/**
		 * 返回部分数据，用于merge
		 */
		@Override
		public Object terminatePartial(AggregationBuffer arg0)
				throws HiveException {
			StudentScore studentScore = (StudentScore) arg0;
			((DoubleWritable) partialResult[0]).set(studentScore.scoreTotal);
			((IntWritable) partialResult[1]).set(studentScore.scoreCount);
			return partialResult;
		}

		/**
		 * 用于合并AggregationBuffer对象，可能来源terminatePartial或是terminate
		 */
		@Override
		public void merge(AggregationBuffer arg0, Object arg1)
				throws HiveException {
			StudentScore studentScore = (StudentScore) arg0;
			studentScore.scoreCount += countOI.get(soi.getStructFieldData(arg1, countField));
			studentScore.scoreTotal += totalOI.get(soi.getStructFieldData(arg1, totalField));
		}
		
		/**
		 * 用于最终处理（Reduce阶段）和返回结果
		 * 这时的AggregationBuffer已经合并了Group里所有的元素，
		 * 可以进行其他处理，例如文本性别分类就在这里执行。
		 */
		@Override
		public Object terminate(AggregationBuffer arg0) throws HiveException {
			StudentScore studentScore = (StudentScore) arg0;
			return new Text(studentScore.getResult());
		}
	}
	public static void main(String[] args){
		System.out.println("ok");
	}
}
