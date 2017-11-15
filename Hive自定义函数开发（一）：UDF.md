# Hive自定义函数开发（一）：UDF

> ### 前言
&emsp;&emsp;Hive拥有很多自带函数[[hive2.0函数大全]](http://www.cnblogs.com/MOBIN/p/5618747.html)，仅仅使用Sql就能完成大部分的操作。但有时候要完成特殊操作的时候就需要自己开发自定义函数实现。

Hive有三种自定义函数：
- UDF：实现一行进一行出的函数功能；
- UDAF：实现多行进一行出的函数功能；
- UDTF：实现多行进多行出的函数功能；

&emsp;&emsp;以上三种可按业务处理需求选择，但开发难度逐级递增。相关的开发资料网上有不少，但难度越高的函数资料越复杂，为了帮助大家和自我成长，也将我在开发中的理解与大家分享。

-----------------------------

> ### 通用JAR包准备
&emsp;&emsp;三种自定义函数用到的包都是一样的，可以一起导入：
- commons-logging-1.1.3.jar
- commons-collections-3.2.2.jar
- gson-2.3.jar
- hadoop-common.jar
- hive-cli.jar
- hive-common.jar
- hive-exec.jar
- hive-jdbc.jar
- hive-metastore.jar
- hive-service.jar
- httpclient-4.2.5.jar
- httpcore-4.2.5.jar
- log4j-1.2.17.jar
- slf4j-api-1.7.5.jar
- slf4j-log4j12.jar

为了方便大家下载，我已经打包好放在云盘：[百度云](https://pan.baidu.com/s/1i5P9XBn)

----------------------

> ### 通用基础数据类型
&emsp;&emsp;基础类型可在`org.apache.hadoop.io`中查看，它们皆继承`Writable`对象，拥有`get`和`set`方法。

常见基础类型有：
- IntWritable 整数型
- LongWritable 长整数
- FloatWritable 单浮点数
- DoubleWritable 双浮点数
- Text 文本

基础用法：
```
public Text fun(Text text) {
    
    FloatWritable f = new FloatWritable((float) 1.2); // 新建单浮点数对象也可直接new FloatWritable()
    float f2 = f.get(); // FloatWritable 转 float
    f.set(f2 * 2); // 重新赋值
    
    String str = text.toString(); // Text转String，Text没有实现get方法
    Text result  = new Text("结果1"); // 新建Text对象
    result.set("结果2"); // 重新赋值
    
    return result;
}
```

&emsp;&emsp;其他基础类型和复合结构类型可在`org.apache.hadoop.io`查看后自行百度查找。

-------------------------

> ### UDF函数开发
&emsp;&emsp;UDF函数是最容易开发的，只需要继承`org.apache.hadoop.hive.ql.exec.UDF`并实现`evaluate`方法即可。
```
package com.franchen.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HiveUDF extends UDF {
    public Text evaluate(Text text) {
        if (null == text) {
            return null;
        }
        // Do some thing
        return text;
    }
    public Text evaluate(Text name, IntWritable age) {
        // 也可以重载多个字段
        // 调用 select test_udf(name, age) from user_table;
        return name;
    }
    public static void main(String[] args){
        HiveUDF udf = new HiveUDF();
        // Do some test
    }
}

```
[[完整代码]](HiveDiyFunction/src/com/franchen/udf/HiveUDF.java)

&emsp;&emsp;若传入的值来自多列（即多参数）且数量不定，则需要用到`org.apache.hadoop.hive.ql.udf.generic.GenericUDF`实现，这个相对复杂，我们下节再讲。

---------------------

> ### 自定义函数使用
&emsp;&emsp;Hive三种自定义函数的使用方法相同：
1. 编写完自定义函数后，导出为可执行jar包。<br>
    &emsp;&emsp;Eclipse中点击：文件-导出-Java-可执行JAR文件-下一步-启动配置选择运行的类（例如：HiveUDF）-选择导出目标（即保存jar的目录）-完成。<br>
    问题1：启动配置无法找到运行的类？<br>
    答：编写的类需要测试运行(main方法)一次后才会出现。

2. 上传至服务器，将JAR包和外部数据加入Hive环境。
    ```
    Hive> add jar 'HiveUDF.jar';
    Hive> add file 'rule.txt'; # 加入外部文件
    Hive> add file 'Data/'; # 加入外部文件夹
    ```
    问题2：自定义函数如何引用外部文件？<br>
    答：首先将外部文件加入Hive环境，然后通过`"./rule.txt"`和`"./Data/data.dat"`就可以读取到文件。
    
3. 创建临时函数并使用。
    ```
    Hive> drop temporary function test_udf; #首先确保函数名没被使用
    Hive> create temporary function test_udf as 'com.franchen.udf.HiveUDF'; #创建临时函数，要完整包名指向可运行类
    Hive> select test_udf(v) from test_table; #使用临时函数
    Hive> drop temporary function test_udf; #使用后记得删除临时函数
    ```
    问题3：为何卡在创建函数不动了？<br>
    答：一、有时创建函数会比较慢，请稍等几分钟；<br>&emsp;&emsp;二、若卡超过十分钟，则是原来使用完临时函数后未删除，导致创建冲突。这种情况下只能尝试能否删除临时函数，或者等Hive自动清除临时函数为止。**为避免这种情况出现，请确保创建临时函数前和使用完临时函数后都要记得删除临时函数！**