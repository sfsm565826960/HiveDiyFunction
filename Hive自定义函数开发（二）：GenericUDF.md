# Hive自定义函数开发（二）：GenericUDF

> ### 前言
&emsp;&emsp;前面我们讲了自定义函数开发涉及的：通用JAR包、通用基础数据类型、通用使用方法，以及简单的UDF函数开发。

&emsp;&emsp;简单的UDF函数只能接收有限个参数，且需要重载`evaluate`方法多次。若业务需要不定参数或`select test_udf(*) from test_table;`则需要用到更复杂的`GenericUDF`实现。

--------------------

> ### GenericUDF实现
&emsp;&emsp;继承`org.apache.hadoop.hive.ql.udf.generic.GenericUDF`需要重写`initialize`、`getDisplayString`、`evaluate`方法。

&emsp;&emsp;为了方便大家理解，这里模拟一个业务场景：编写一个通用UDF实现计算同学成绩的总分和平均分。
- 输入：姓名、成绩1、成绩2...（假设每次考试考的科目数不定）
- 输出：`Hello, {{ name }}. You took {{ scoreCount }} courses.Total score is {{ scoreTotal }}, and average score is {{ scoreAverage }}.`

&emsp;&emsp;具体请查看[[完整代码]]()，下面分别对这三个方法进行解释：

##### initialize
```
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
	// 再细致些还可以通过arg0[i].getTypeName()检查参数是否为string、double等等
	
	// 2.设置输入类型，存储在全局变量中
	nameOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	scoreOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
	// scoreOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	
	// 3.初始化全局变量
	result = new Text();
	
	// 4.返回输出类型
	return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
}
```

##### evaluate
```
public Text evaluate(DeferredObject[] arg0) throws HiveException {
	// 这里实现UDF功能，函数返回类型根据需要自己改
	double scoreTotal = 0;

	// 获取数据
	String name = PrimitiveObjectInspectorUtils.getString(arg0[0].get(), nameOI);
	for(int i = 1; i < arg0.length; i++) {
		scoreTotal += PrimitiveObjectInspectorUtils.getDouble(arg0[i].get(), scoreOI);
	}
	
	// 计算平均值
	int scoreCount = arg0.length - 1; // arg0.length > 1
	double scoreAverage = scoreTotal / scoreCount;
	
	// 返回结果
	result.set(
		"Hello, " + name + "."
		+ "You took " + scoreCount + " courses."
		+ "Total score is " + scoreTotal
		+ ", and average score is " + scoreAverage + "."
	);
	
	return result;
}
```

##### getDisplayString
```
public String getDisplayString(String[] arg0) {
	// 这里用于解释SQL的时候，方便查看返回格式。
	// 并不是重要方法，若无特殊需求，简单写就行。
	return "test_generic_udf(" + arg0.toString() + ")";
}
```

---------------

> ### 测试
```
Hive> add jar /home/rcgzz/cdc/HiveGenericUDF.jar;
Hive> drop temporary function test_generic_udf;
Hive> create temporary function test_generic_udf as 'com.franchen.genericudf.HiveGenericUDF';

Hive> select test_generic_udf("Franchen1", 92.0, 86.5, 100.0, 96.0);
Hello, Franchen1.You took 4 courses.Total score is 374.5, and average score is 93.625.

Hive> select test_generic_udf(*) from (select "Franchen2", 91.5, 99.0, 88.5) t1;
Hello, Franchen1.You took 3 courses.Total score is 279.0, and average score is 93.0.

Hive> drop temporary function test_generic_udf;
```

在测试过程中，你可能会发现错误类似：
```
Hive> select test_generic_udf("Franchen1", 92, 86.5, 100, 96);
FAILED: ClassCastException org.apache.hadoop.io.IntWritable cannot be cast to org.apache.hadoop.hive.serde2.io.DoubleWritable
```
原因：我们读取的第二个参数是`92`，被定义成`IntWritable`，但`IntWritable`和`DoubleWritable`不支持互转（原因猜测是来源包不同），因此转换失败。

解决方法：
1. 确保输入来源统一是`Double`，但这个不是很好的方法。

2. `IntWritable`和`DoubleWritable`都可以转为`Text`类型，而`Text`可以转为`Double`。因此以`Text`为中间层进行转换，具体修改`initialize`的`scoreOI`为`PrimitiveObjectInspectorFactory.javaStringObjectInspector`即可。

3. 确保`IntWritable`和`DoubleWritable`来源包一致。（未测试过，若读者测试可以，欢迎留言\^_^）