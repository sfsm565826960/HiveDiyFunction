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
		 * �����info���ܸ���ȫ�棬�����жϵĹ��ܸ��ࡣ
		 * ����info.isAllColumns()��info.isDistinct()
		 * ��getEvaluator(TypeInfo[] info)�о�û�С�
		 * ͨ��info.getParameters()���ɻ�����غ�����TypeInfo[] info��
		 * ֮������ʵ��getEvaluator(TypeInfo[] info)����Ϊ���߼������������ࡣ
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
		 * ����ʵ�ֶԲ������������͵ļ�飬
		 * �����ݲ������͵��ò�ͬ��GenericUDAFEvaluator
		 */
		
		// 1. ����������������
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
		
		// 2. �������󷵻ز�ͬ��GenericUDAFEvaluator��
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
		 * ��ʼ��GenericUDAFEvaluator
		 */
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			// 1. �������ֻ��һ�������򱨴�
			assert (parameters.length == 1);
			// 2. ��ʼ������
			super.init(m, parameters);
			
			// 3. ������������
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				// �ڶ���˵����ʹ��Text��ΪIntWritable��DoubleWritable���м��
				inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			} else {
				soi = (StructObjectInspector) parameters[0];
				totalField = soi.getStructFieldRef("total");
				countField = soi.getStructFieldRef("count");
				totalOI = (DoubleObjectInspector) totalField.getFieldObjectInspector();
				countOI = (IntObjectInspector) countField.getFieldObjectInspector();
			}
			
			// 4. ���÷�������
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
		
		// �̳�AggregationBuffer��ʵ�����ݶ���
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
		 * �����µ�AggregationBuffer����
		 */
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			// TODO �Զ����ɵķ������
			return new StudentScore();
		}
		
		/**
		 * ����AggregationBuffer����
		 */
		@Override
		public void reset(AggregationBuffer arg0) throws HiveException {
			StudentScore studentScore = (StudentScore) arg0;
			studentScore.reset();
		}

		/**
		 * ����һ������
		 */
		@Override
		public void iterate(AggregationBuffer arg0, Object[] arg1)
				throws HiveException {
			assert(arg1.length == 1);
			// ��������
			if (null != arg1 && null != arg1[0]) {
				double score = PrimitiveObjectInspectorUtils.getDouble(arg1[0], inputOI);
				StudentScore studentScore = (StudentScore) arg0;
				studentScore.addScore(score);
			}
		}
		
		/**
		 * ���ز������ݣ�����merge
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
		 * ���ںϲ�AggregationBuffer���󣬿�����ԴterminatePartial����terminate
		 */
		@Override
		public void merge(AggregationBuffer arg0, Object arg1)
				throws HiveException {
			StudentScore studentScore = (StudentScore) arg0;
			studentScore.scoreCount += countOI.get(soi.getStructFieldData(arg1, countField));
			studentScore.scoreTotal += totalOI.get(soi.getStructFieldData(arg1, totalField));
		}
		
		/**
		 * �������մ���Reduce�׶Σ��ͷ��ؽ��
		 * ��ʱ��AggregationBuffer�Ѿ��ϲ���Group�����е�Ԫ�أ�
		 * ���Խ����������������ı��Ա�����������ִ�С�
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
