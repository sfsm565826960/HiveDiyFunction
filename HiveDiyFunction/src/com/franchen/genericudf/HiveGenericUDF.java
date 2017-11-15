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
		// initialize 需要做四件事
		
		// 1.检查参数数量及类型
		if (arg0.length < 2) {
			throw new UDFArgumentException("This function expect more than one argument.");
		}
		// 设置所有参数仅支持原始类型
		for (int i = 0; i < arg0.length; i++) {
			if (arg0[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i,
						"Only primitive type arguments are accepted but "  
				          + arg0[i].getTypeName() + " is passed.");
			}
		}
		
		// 2.存储在全局变量的ObjectInspectors元素的输入
		nameOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//		scoreOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		scoreOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		
		// 3.初始化全局变量
		result = new Text();
		
		// 4.返回变量输出类型
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	@Override
	public Text evaluate(DeferredObject[] arg0) throws HiveException {
		// 这里实现UDF功能

		// 参数最好通过下面方式获取
		
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
		// 这里用于解释SQL的时候，方便查看返回格式
		return "test_generic_udf(" + arg0.toString() + ")";
	}

	public static void main(String[] args) {
		System.out.println("ok");
	}
}
