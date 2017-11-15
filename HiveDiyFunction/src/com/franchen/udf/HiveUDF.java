package com.franchen.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HiveUDF extends UDF {
    public Text evaluate(Text name) {
        if (null == name) {
            return null;
        }
        // Do some thing
        String nameString = name.toString();
        name.set("hello, " + nameString + ".");
        return name;
    }
    public Text evaluate(Text name, IntWritable age) {
        // 也可以重载多个字段
        // 例如：调用 select test_udf(name, age) from user_table;
    	Text text = new Text();
    	text.set("hello, " + name.toString() + ", you are " + age.get() + " age old.");
        return text;
    }
    public static void main(String[] args){
        HiveUDF udf = new HiveUDF();
        // Do some test
        System.out.println(
        	udf.evaluate(new Text("franchen")).toString()
        ); // hello, franchen.
        System.out.println(
        	udf.evaluate(new Text("franchen"), new IntWritable(23)).toString()
        ); // hello, franchen, you are 23 age old.
    }
}