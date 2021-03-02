package com.yykj.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {

    /**
     * 创建文件夹
     */
    @Test
    public void hdfsMkDir() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");
        fs.mkdirs(new Path("/0808/first"));
        fs.close();

        System.out.println("finish !!!");
    }

    @Test
    public void hdfsTouchFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");
        FSDataOutputStream stream = fs.create(new Path("/0808/first/test"), true);
        stream.writeUTF("11111111111111111111111111");
        stream.flush();
        stream.close();
    }

    /**
     * 上传文件
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void hdfsFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        //conf.set("dfs.replication", "2");

        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf , "kelvin");
        fs.copyFromLocalFile(new Path("e:/hello.txt"), new Path("/hello2.txt"));
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     * 下载文件
     */
    @Test
    public void hdfsCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");
        //fs.copyToLocalFile(new Path("/hello.txt"), new Path("e:/hello.txt"));
        //使用本地系统 不用校验.crc
        fs.copyToLocalFile(false, new Path("/hello.txt"), new Path("e:/hello.txt"), true);
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     * 删除文件或文件夹
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsDelete() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        //第二个参数标识是否为文件夹
        fs.delete(new Path("/hello3.txt"), false);
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     * 重命名文件
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsRename() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        //第二个参数标识是否为文件夹
        fs.rename(new Path("/hello2.txt"), new Path("/hello3.txt"));
        fs.close();

        System.out.println("finish !!!");
    }

    /**
     * 获取文件信息
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsListFiles() throws FileNotFoundException, IllegalArgumentException, IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        //参数recursive:是否递归
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blocks = fileStatus.getBlockLocations();
            for(BlockLocation info : blocks){
                String[] hosts = info.getHosts();

                for(String host : hosts){
                    System.out.println(host);
                }
            }
        }

        fs.close();
        System.out.println("finish !!!");
    }

    /**
     * 判断是否为文件夹
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void hdfsListStatus() throws FileNotFoundException, IllegalArgumentException, IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node21:9000"), conf, "kelvin");

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus status : listStatus){
            if(status.isFile()){
                System.out.println(status.getPath().getName() + "：文件");
            }
            else{
                System.out.println(status.getPath().getName() + "：文件夹");
            }
        }

        fs.close();
        System.out.println("finish !!!");
    }
}
