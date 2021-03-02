package com.yykj.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HDFSIO {

    /**
     * 数据流：将本次文件上传至HDFS
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsIOUtils() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        FileInputStream fis = new FileInputStream("e:/hello.txt");
        FSDataOutputStream fos = fs.create(new Path("/hello.txt"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     * 数据流：将HDFS文件下载到本地
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsIOUtils2() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        FSDataInputStream fis = fs.open(new Path("/hello.txt"));
        FileOutputStream fos = new FileOutputStream(new File("e:/hello.txt"));

        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     * 数据流：下载第一块HDFS文件
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsFileSeek1() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.0.tar.gz._COPYING_"));
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.10.0.tar.gz._COPYING_.part1"));

        byte[] buf = new byte[1024];
        for(int i = 0; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     *  数据流：下载第二块HDFS文件
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsFileSeek2() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.0.tar.gz._COPYING_"));
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.10.0.tar.gz._COPYING_.part2"));

        fis.seek(1024*1024*128);
        IOUtils.copyBytes(fis, fos, conf);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

        System.out.println("finish !!!");
    }
}
