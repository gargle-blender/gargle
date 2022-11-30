package com.gargle.common.utils.hdfs;

import com.alibaba.fastjson.JSONObject;
import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.file.FileUtil;
import com.gargle.common.utils.string.StringUtil;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.GroupType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * ClassName:HDFSUtil
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/29 15:34
 */
@Data
@NoArgsConstructor
public final class HDFSUtil {

    private static final Logger logger = LoggerFactory.getLogger(HDFSUtil.class);

    public static HDFSUtil getInstance() {
        return HDFSUtil.Holder.hdfsUtil;
    }

    private static class Holder {
        public static final HDFSUtil hdfsUtil = new HDFSUtil();
    }

    private FileSystem fileSystem;

    private String hdfsUrl;

    private Map<String, String> conf;

    private String user;

    public void init(String hdfsUrl, Map<String, String> conf, String user) {
        this.conf = conf;
        this.user = user;
        hdfsUrl = hdfsUrl.trim();
        this.hdfsUrl = hdfsUrl;
        buildFileSystem(hdfsUrl, conf, user);
        logger.info("hdfs 初始化完毕: " + this);
    }

    public void close() throws IOException {
        fileSystem.close();
    }

    public String testFile(String path) throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path(path));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件:" + status.getPath().getName());
                return status.getPath().getName();
            } else {
                System.out.println("目录:" + status.getPath().getName());
            }
        }

        return null;
    }

    public void download(String localPath, String hdfsPath) throws IOException {
        FSDataInputStream fis = fileSystem.open(new Path(getHdfsPath(hdfsPath)));
        FileOutputStream fos = new FileOutputStream(localPath, true);
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    public boolean exists(String path) {
        try {
            return fileSystem.exists(new Path(path));
        } catch (IOException e) {
            throw new GargleException("调用 hdfs 判断文件是否存在异常!", e);
        }
    }

    public void download(boolean delSrc, String srcFilePath, String localPath) throws IOException {
        /*
         *  参数解读： 参数一：源文件是否删除  参数二：源文件的路径  参数三：目标路径  参数四：是否关闭校验
         */
        fileSystem.copyToLocalFile(delSrc, new Path(getHdfsPath(srcFilePath)), new Path(localPath), true);
    }

    public void deleteFile(String filePath) throws IOException {
        // 参数解读： 参数一：要删除的路径  参数二：是否递归删除
        fileSystem.delete(new Path(filePath), false);
    }

    public void deleteDir(String dirPath) throws IOException {
        // 参数解读： 参数一：要删除的路径  参数二：是否递归删除
        fileSystem.delete(new Path(dirPath), true);
    }

    public void upload(String localPath, String hdfsPath) throws IOException {
        Path path = new Path(getHdfsPath(hdfsPath));
        if (fileSystem.exists(path)) {
            logger.warn("{}-文件已存在,", getHdfsPath(hdfsPath));
            return;
        }
        //获取HDFS文件系统的输出流
        FSDataOutputStream fos = fileSystem.create(path);
        //获取本地文件的输入流
        FileInputStream fis = new FileInputStream(localPath);
        //上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    public void mkdirs(String dirPath) {
        try {
            fileSystem.mkdirs(new Path(dirPath));
        } catch (IOException e) {
            logger.error("创建文件夹: [{}] 异常!", dirPath, e);
            throw new GargleException(e);
        }
    }

    public List<String> downloadParquet(String tableName, String tablePath, String localPath) {
        List<String> localFileNames = new ArrayList<>();
        Set<String> files = new HashSet<>();
        localPath = localPath + tableName + "/";
        FileUtil.createDirectory(localPath);
        String hdfsPath = tablePath + tableName + "/";
        if (!exists(hdfsPath)) {
            logger.warn("无需要下载的Parquet文件: hdfsUrl: {}, hdfsPath:{} ",
                    hdfsUrl, hdfsPath);
            return localFileNames;
        }
        FileStatus[] listStatus;
        try {
            listStatus = fileSystem.listStatus(new Path(hdfsPath));
        } catch (IOException e) {
            throw new GargleException("listStatus 异常!" + e.getMessage(), e);
        }
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                files.add(status.getPath().getName());
            } else {
                throw new GargleException(hdfsPath + ", 存在目录" + status.getPath().getName());
            }
        }
        for (String file : files) {
            String fileName = hdfsPath + file;
            String localFileName = localPath + file;
            FileUtil.deleteFile(localFileName);
            try {
                localFileNames.add(localFileName);
                download(localFileName, fileName);
            } catch (IOException e) {
                throw new GargleException("hdfs下载文件失败!" + e.getMessage(), e);
            }
        }

        return localFileNames;
    }

    public long readParquet(String filePath, String key, Consumer<String> consumer) throws IOException {
        AtomicLong count = new AtomicLong(0);
        Path file = new Path(filePath);
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);
        ParquetReader<Group> reader = builder.build();
        SimpleGroup group = null;
        GroupType groupType = null;
        while ((group = (SimpleGroup) reader.read()) != null) {
            if (groupType == null) {
                groupType = group.getType();
            }
            for (int i = 0; i < groupType.getFieldCount(); i++) {
                String tmpName = groupType.getFieldName(i);
                if (!StringUtil.isBlank(key) && key.equals(tmpName)) {
                    try {
                        String tmp = group.getValueToString(i, 0);
                        if (StringUtil.isNotBlank(tmp)) {
                            count.incrementAndGet();
                            consumer.accept(tmp);
                        }
                    } catch (Exception e) {
                        throw new GargleException("readParquet 异常!" + e.getMessage(), e);
                    }

                }
            }

        }
        return count.get();
    }

    private void buildFileSystem(String hdfsUrl, Map<String, String> conf, String user) {
        try {
            this.fileSystem = FileSystem.get(getURI(hdfsUrl), getConfiguration(conf), user);
        } catch (IOException | InterruptedException e) {
            logger.error("hdfsUrl: {}, user: {}, conf: {} 创建FileSystem异常!", hdfsUrl, user, JSONObject.toJSONString(conf), e);
            throw new GargleException(e);
        }
    }

    private static String getUser(String user) {
        return user;
    }

    private static URI getURI(String hdfsUrl) {
        try {
            return new URI(hdfsUrl);
        } catch (URISyntaxException e) {
            logger.error("创建URI链接异常, HDFSUrl: {}", hdfsUrl, e);
            throw new GargleException("创建URI链接异常, HDFSUrl: " + hdfsUrl, e);
        }
    }

    private static Configuration getConfiguration(Map<String, String> conf) {
        Configuration configuration = new Configuration();
        if (conf != null && conf.size() > 0) {
            conf.forEach(configuration::set);
        }
        return configuration;
    }

    private String getHdfsPath(String path) {
        return hdfsUrl + path;
    }

    private static String getUrl(String hdfsIp, String hdfsPort) {
        return "hdfs://" + hdfsIp + ":" + hdfsPort;
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}
