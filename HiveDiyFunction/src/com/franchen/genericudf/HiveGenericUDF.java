package com.franchen.genericudf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

public class HiveGenericUDF extends GenericUDF {
	
	PrimitiveObjectInspector nameOI;
	PrimitiveObjectInspector scoreOI;
	Text result;
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0)
			throws UDFArgumentException {
		// initialize ��Ҫ���ļ���
		
		// 1.����������������
		if (arg0.length < 2) {
			throw new UDFArgumentException("This function expect more than one argument.");
		}
		// �������в�����֧��ԭʼ����
		for (int i = 0; i < arg0.length; i++) {
			if (arg0[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i,
						"Only primitive type arguments are accepted but "  
				          + arg0[i].getTypeName() + " is passed.");
			}
		}
		
		// 2.�洢��ȫ�ֱ�����ObjectInspectorsԪ�ص�����
		nameOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//		scoreOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		scoreOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		
		// 3.��ʼ��ȫ�ֱ���
		result = new Text();
		
		// 4.���ر����������
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	@Override
	public Text evaluate(DeferredObject[] arg0) throws HiveException {
		// ����ʵ��UDF����

		// �������ͨ�����淽ʽ��ȡ
		
		String name = PrimitiveObjectInspectorUtils.getString(arg0[0].get(), nameOI);
		
		double scoreTotal = 0;
		for(int i = 1; i < arg0.length; i++) {
			scoreTotal += PrimitiveObjectInspectorUtils.getDouble(arg0[i].get(), scoreOI);
		}
		int scoreCount = arg0.length - 1; // arg0.length > 1
		double scoreAverage = scoreTotal / scoreCount;
		
		result.set(
			"Hello, " + name + "."
			+ "You took " + scoreCount + " courses."
			+ "Total score is " + scoreTotal
			+ ", and average score is " + scoreAverage + "."
		);
		
		return result;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// �������ڽ���SQL��ʱ�򣬷���鿴���ظ�ʽ
		return "test_generic_udf(" + arg0.toString() + ")";
	}

	public static void main(String[] args) {
		System.out.println("ok");
	}
}
