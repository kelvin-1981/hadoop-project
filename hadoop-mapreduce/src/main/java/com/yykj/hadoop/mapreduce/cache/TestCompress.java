package com.yykj.hadoop.mapreduce.cache;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        //1.测试BZip2Codec
        //compress("e:/test/input15/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");

        //2.测试Gzip
        //compress("e:/test/input15/hello.txt","org.apache.hadoop.io.compress.GzipCodec");

        //3.解压缩
        deCompress("e:/test/input15/hello.txt.bz2");
    }



    @SuppressWarnings({ "unchecked", "unused" })
    private static void compress(String fileName, String method) throws ClassNotFoundException, IOException {

        //1.获取输入流
        FileInputStream fis = new FileInputStream(fileName);


        @SuppressWarnings("rawtypes")
        Class codeClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass , new Configuration());

        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(fileName + codec.getDefaultExtension());
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //3.数据对拷
        //缓冲区（自定义）：1024 * 1024 * 5
        //false:复制结束后是否关闭输入流及输出流
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

        //4.关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cos);
    }

    private static void deCompress(String fileName) throws FileNotFoundException, IOException {

//		//1.压缩方式检查
//		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
//		CompressionCodec codec = factory.getCodec(new Path(fileName));
//		if(codec == null){
//			System.out.println("未发现压缩方式！");
//			return;
//		}
//
//		//2.获取输入流
//		CompressionInputStream cis = codec.createInputStream(new FileInputStream(fileName));
//
//		//3.生成输出流
//		FileOutputStream fos = new FileOutputStream(fileName + ".decode");
//
//		//4.数据对拷
//		IOUtils.copyBytes(cis, fos, null);
//
//		//5.关闭资源
//		IOUtils.closeStream(cis);
//		IOUtils.closeStream(fos);
    }

}