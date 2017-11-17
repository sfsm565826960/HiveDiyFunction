# Hive自定义函数开发（三）：GenericUDAF

> ### 前言
&emsp;&emsp;前面两节我们详细介绍了一行进一行出的UDF，它能适用于大部分的业务场景。然而对于需要分组合并运算（涉及Group By）的场景就不太适用，这时候就需要用到UDAF。

----------

> ### UDAF 与 GenericUDAF 的区别

&emsp;&emsp;UDAF继承`org.apache.hadoop.hive.ql.exec.UDAF`类，并在派生类中以静态内部类的方式实现`org.apache.hadoop.hive.ql.exec.UDAFEvaluator`接口。这种方式简单直接，但是在使用过程中需要依赖JAVA反射机制，因此性能相对较低。现在该接口已经被注解为Deprecated，建议不要使用这种方式开发新的UDAF函数。

GenericUDF是Hive社区推荐的新的写法，以抽象类代替原有的接口。新的抽象类`org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver`替代老的UDAF接口，新的抽象类`org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator`替代老的UDAFEvaluator接口。

-------------

> ### UDAF的运行流程简介

- init: 实例化Evaluator类的时候调用的，在不同的阶段需要返回不同的OI。

  init函数会带Mode参数表示在哪个阶段，Mode是枚举值，各值含义如下：

  - PARTIAL1：原始数据到部分聚合，调用iterate和terminatePartial --> map阶段
  
  - PARTIAL2: 部分聚合到部分聚合，调用merge和terminatePartial --> combine阶段
  
  - FINAL: 部分聚合到完全聚合，调用merge和terminate --> reduce阶段
  
  - COMPLETE: 从原始数据直接到完全聚合 --> map阶段，并且没有reduce

  其入参和返回值，以及Mode阶段的关系如下表：

  | Mode |     入参    | 返回值的使用者|
  |------|-------------|--------------|
  | P1   | 原始数据     | terminatePartial|
  | P2   | 部分聚合数据 | terminatePartial|
  | F    |部分聚合数据  |terminate|
  | C    |原始数据      |terminate|

- getNewAggregationBuffer: 获取存放中间结果的对象

- iterate：处理一行数据

- terminatePartial：返回部分聚合数据的持久化对象。因为调用这个方法时，说明已经是map或者combine的结束了，必须将数据持久化以后交给reduce进行处理。只支持JAVA原始数据类型及其封装类型、HADOOP Writable类型、List、Map，不能返回自定义的类，即使实现了Serializable也不行，否则会出现问题或者错误的结果。

- merge：将terminatePartial返回的部分聚合数据进行合并，需要使用到对应的OI。

- terminate：结束，生成最终结果。

-----------

> ### GenericUDAF的实现

&emsp;&emsp;UDAF的开发比UDF的开发要复杂许多，为了能让大家更直观的理解，我们依旧用第二节计算考试分数的场景，但表格有所不同：
```
score_table:
| name | subject | score |
+------+---------+-------+
| Jack | english |  80.0 |
| Tom  | english |  84.0 |
| Jack | chinese |  92.0 |
| Tom  | chinese |  90.0 |
| Jack |   math  |  99.5 |
| Tom  |   math  | 100.0 |

Hive> select name, test_udaf(score) from score_table group by name;

Hive> select subject, test_udaf(score) from score_table group by subject;
```
[[完整代码实现]](HiveDiyFunction/src/com/franchen/genericudaf/HiveGenericUDAF.java)

GenericUDAF主要继承并实现`org.apache.hadoop.hive.ql.udf.generic`中两个类的方法：
- `AbstractGenericUDAFResolver`：该抽象类实现了GenericUDAFResolver2的接口。UDAF主类须继承该抽象类，其主要作用是实现参数类型检查和操作符重载。可以为同一个函数实现不同入参的版本。
- `GenericUDAFEvaluator`：该抽象类为UDAF具体的逻辑处理，包括几个必须实现的抽象方法，这几个方法负责完成UDAF所需要处理的逻辑。


1. ### `AbstractGenericUDAFResolver`的实现：
```
package com.franchen.genericudaf;

// 省略import代码

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
      throw new UDFArgumentTypeException(1,  
                "Only primitive type arguments are accepted but "  
                + info[1].getTypeName() + " is passed."); 
    }

    // 2. 根据需求返回不同的GenericUDAFEvaluator类
    switch (((PrimitiveTypeInfo) info[1]).getPrimitiveCategory()) {
      case INT:
        // return new IntGenericUDAF();
      case DOUBLE:
        // return new DoubleGenericUDAF();
      case STRING:
        return new StringGenericUDAF();
      default:
        throw new UDFArgumentTypeException(0,  
                  "Only string or double or int type arguments are accepted but "  
                  + info[0].getTypeName() + " is passed.");
    }
    
    return new StringGenericUDAF();
  }

  public static class StringGenericUDAF extends GenericUDAFEvaluator{
    // ....
  }
}
```

2. ### `GenericUDAFEvaluator`的实现：
```
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
      // 中间结构体
      soi = (StructObjectInspector) parameters[0];
      totalField = soi.getStructFieldRef("total");
      countField = soi.getStructFieldRef("count");
      totalOI = (DoubleObjectInspector) totalField.getFieldObjectInspector();
      countOI = (IntObjectInspector) countField.getFieldObjectInspector();
    }

    // 4. 设置输出类型
    if (m == Mode.FINAL || m == Mode.COMPLETE) {
      result = new Text();
      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    } else {
      partialResult = new Object[2];
      partialResult[0] = new DoubleWritable(0);
      partialResult[1] = new IntWritable(0);
      // 中间结构体
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
    public StudentScore(){ reset(); }
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
    * 创建新的AggregationBuffer对象
    */
  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
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
    * 返回已处理的部分数据，用于merge
    */
  @Override
  public Object terminatePartial(AggregationBuffer arg0)
      throws HiveException {
    // 返回中间结构体
    StudentScore studentScore = (StudentScore) arg0;
    ((DoubleWritable) partialResult[0]).set(studentScore.scoreTotal);
    ((IntWritable) partialResult[1]).set(studentScore.scoreCount);
    return partialResult;
  }

  /**
    * 用于合并AggregationBuffer对象
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
```

---------

> ### 测试
首先创建表：
```
create table score_table as select t._c0 as name, t._c1 as subject, t._c2 as score from (select "Jack","english",80.0 union all select "Tom","english",84.0 union all select "Jack","chinese",92.0 union all select "Tom","chinese",90.0 union all select "Jack","math",99.5 union all select "Tom","math",100.0) t;

score_table:
| name | subject | score |
+------+---------+-------+
| Jack | english |  80.0 |
| Tom  | english |  84.0 |
| Jack | chinese |  92.0 |
| Tom  | chinese |  90.0 |
| Jack |   math  |  99.5 |
| Tom  |   math  | 100.0 |
```

运行GenericUDAF:
```
Hive> add jar HiveGenericUDAF.jar;
Hive> drop temporary function test_generic_udaf;
Hive> create temporary function test_generic_udaf as 'com.franchen.genericudaf.HiveGenericUDAF';

Hive> select name, test_generic_udaf(score) from score_table group by name;
Jack	This student took 3 subject, total score is 271.5, average score is 90.5.
Tom	This student took 3 subject, total score is 274.0, average score is 91.33333333333333.

Hive> select subject, test_udaf(score) from score_table group by subject;
chinese	This student took 2 subject, total score is 182.0, average score is 91.0.
english	This student took 2 subject, total score is 164.0, average score is 82.0.
math	This student took 2 subject, total score is 199.5, average score is 99.75.
# 这里表达未做优化，但不影响功能，先将就着用吧...

Hive> drop temporary function test_generic_udaf;
```
