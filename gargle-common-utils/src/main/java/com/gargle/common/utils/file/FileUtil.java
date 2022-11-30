package com.gargle.common.utils.file;

import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.string.StringUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

/**
 * ClassName:FileUtil
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/29 14:24
 */
public final class FileUtil {

    /**
     * 创建文件夹
     *
     * @param path 需要创建的文件夹路径
     */
    public static synchronized void createDirectory(String path) {
        File file = new File(path);
        if (!file.exists()) {
            boolean mkdirs = file.mkdirs();
            if (!mkdirs) {
                throw new GargleException("创建文件夹: " + path + " , 失败");
            }
        } else {
            if (file.isFile()) {
                throw new GargleException("存在同名文件: " + path);
            }
        }
    }

    /**
     * 创建文件
     *
     * @param path     文件所在文件夹路径
     * @param fileName 需要创建的文件名称
     */
    public static synchronized void createFile(String path, String fileName) {
        createDirectory(path);
        String filePath = path + File.separatorChar + fileName;
        File file = new File(filePath);
        if (file.exists()) {
            try {
                boolean newFile = file.createNewFile();
                if (!newFile) {
                    throw new GargleException("创建文件未成功.");
                }
            } catch (Exception e) {
                throw new GargleException("创建文件失败, filePath: " + filePath + ", e: " + e.getMessage(), e);
            }
        } else {
            if (file.isDirectory()) {
                throw new GargleException("存在同名文件夹: " + filePath);
            }
        }
    }

    /**
     * 删除文件
     *
     * @param filePath 文件路径
     */
    public static synchronized void deleteFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return;
        }

        if (file.isFile()) {
            try {
                boolean delete = file.delete();
                if (!delete) {
                    throw new GargleException("删除文件" + filePath + "失败");
                }
            } catch (Exception e) {
                throw new GargleException("删除文件" + filePath + "失败, e: " + e.getMessage(), e);
            }
        }

    }

    /**
     * 单行追加写入文件
     */
    public static synchronized void writeFile(String pathName, String message) {
        if (StringUtil.isBlank(message) || message.trim().toLowerCase(Locale.ROOT).equals("null")) {
            return;
        }
        File file = new File(pathName);
        if (!file.exists()) {
            try {
                boolean newFile = file.createNewFile();
                if (!newFile) {
                    throw new GargleException("文件创建失败 : " + pathName);
                }
            } catch (Exception e) {
                throw new GargleException("文件创建失败 : " + pathName);
            }


        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(pathName, true), StandardCharsets.UTF_8)
        ); PrintWriter printWriter = new PrintWriter(bufferedWriter)) {
            printWriter.println(message);
            printWriter.flush();
            bufferedWriter.flush();
        } catch (Exception exception) {
            throw new GargleException("数据写入文件: " + pathName + "失败." + exception.getMessage(), exception);
        }
    }

    /**
     * 批量追加写入文件
     */
    public static synchronized void writeFile(String pathName, List<String> messages) {
        if (messages == null || messages.size() == 0) {
            return;
        }

        File file = new File(pathName);
        if (!file.exists()) {
            try {
                boolean newFile = file.createNewFile();
                if (!newFile) {
                    throw new GargleException("文件创建失败 : " + pathName);
                }
            } catch (Exception e) {
                throw new GargleException("文件创建失败 : " + pathName, e);
            }

        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(pathName, true), StandardCharsets.UTF_8)
        ); PrintWriter printWriter = new PrintWriter(bufferedWriter)) {
            for (String message : messages) {
                if (StringUtil.isBlank(message) || message.trim().toLowerCase(Locale.ROOT).equals("null")) {
                    continue;
                }
                printWriter.println(message);
                printWriter.flush();
                bufferedWriter.flush();
            }
        } catch (Exception exception) {
            throw new GargleException("数据写入文件: " + pathName + "失败." + exception);
        }
    }

    /**
     * 获取文件的第一行非空数据.
     */
    public static String readLine(String pathName) {
        File file = new File(pathName);
        if (!file.exists()) {
            return null;
        }

        try (InputStream inputStream = new FileInputStream(file);
             Reader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             Scanner sc = new Scanner(fileReader)) {
            while (sc.hasNextLine()) {  //按行读取字符串
                String line = sc.nextLine();
                if (StringUtil.isNotBlank(line)) {
                    return line;
                }
            }
        } catch (Exception e) {
            throw new GargleException(e);
        }

        return "";
    }

    /**
     * 目标文件中包含key的行写入其他文件
     *
     * @param pathName      目标文件
     * @param key           索引key值
     * @param writePathName 其他文件
     */
    public static String readFile(String pathName, String key, String writePathName) {
        StringBuilder stringBuilder = new StringBuilder();
        File file = new File(pathName);
        if (!file.exists()) {
            return stringBuilder.toString();
        }

        long fileLineNum = getFileLineNum(pathName);
        System.out.println("总行数: " + fileLineNum);
        long i = 0;
        try (InputStream inputStream = new FileInputStream(file);
             Reader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             Scanner sc = new Scanner(fileReader)) {
            while (sc.hasNextLine()) {  //按行读取字符串
                String line = sc.nextLine();
                i++;
                if (i % 10_0000 == 0) {
                    System.out.println("已读取: " + i + " 行");
                }
                if (StringUtil.isNotBlank(key)) {
                    if (StringUtil.isBlank(line)) {
                        continue;
                    }
                    if (line.contains(key)) {
                        System.out.println("包含插入");
                        stringBuilder.append(line).append("\n");
                    }
                } else {
                    stringBuilder.append(line).append("\n");
                }
            }
        } catch (Exception e) {
            throw new GargleException(e);
        }

        if (StringUtil.isNotBlank(writePathName)) {
            writeFile(writePathName, stringBuilder.toString());
        }

        return stringBuilder.toString();
    }

    /**
     * 读取文件行数.
     */
    private static long getFileLineNum(String pathName) {
        try (InputStream inputStream = new FileInputStream(pathName);
             Reader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             LineNumberReader lineNumberReader = new LineNumberReader(fileReader)) {

            lineNumberReader.skip(Long.MAX_VALUE);
            return lineNumberReader.getLineNumber();
        } catch (Exception e) {
            throw new GargleException(e, "读取文件: %s 行数异常", pathName);
        }
    }
}
