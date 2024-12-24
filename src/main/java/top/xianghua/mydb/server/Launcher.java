package top.xianghua.mydb.server;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import top.xianghua.mydb.server.dm.DataManager;
import top.xianghua.mydb.server.server.Server;
import top.xianghua.mydb.server.tbm.TableManager;
import top.xianghua.mydb.server.tm.TransactionManager;
import top.xianghua.mydb.server.utils.Panic;
import top.xianghua.mydb.server.vm.VersionManager;
import top.xianghua.mydb.server.vm.VersionManagerImpl;
import top.xianghua.mydb.common.Error;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.nio.MappedByteBuffer;


public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1<<20)*64;
    public static final long KB = 1 << 10;
	public static final long MB = 1 << 20;
	public static final long GB = 1 << 30;

    // 用于跟踪数据库连接状态
    // 使用AtomicBoolean来保证线程安全和全局唯一性
//    private static AtomicBoolean isDatabaseConnected = new AtomicBoolean(false);
    private static final File SHARED_MEM_FILE = new File("shared_mem.dat");
    private static final int BUFFER_SIZE = 1;


    public static volatile boolean isDatabaseConnected = false;

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        options.addOption("delete", true, "-delete DBPath"); // 新增删除选项
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        if(cmd.hasOption("delete")) {
            deleteDB(cmd.getOptionValue("delete")); // 新增删除操作
        }
        System.out.println("Usage: launcher (open|create|delete) DBPath");
    }

    private static void createDB(String path) {
        // 检查路径是否存在
        File directory = new File(path);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                System.err.println("无法创建目录: " + path);
                return;
            }
        }
        // 获取数据库文件应当存放的目录
        String dbFilePath = path+File.separator+path.substring(path.lastIndexOf(File.separator)+1);
        TransactionManager tm = TransactionManager.create(dbFilePath);
        DataManager dm = DataManager.create(dbFilePath, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(dbFilePath, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) throws IOException {
        // 1.检查路径是否存在
        File directory = new File(path);
        if (!directory.exists()) {
            System.err.println("该数据库路径"+path+"不存在,请先创建数据库");
            return;
        }

        // 获取数据库文件应当存放的目录
        String dbFilePath = path+File.separator+path.substring(path.lastIndexOf(File.separator)+1);
        TransactionManager tm = TransactionManager.open(dbFilePath);
        DataManager dm = DataManager.open(dbFilePath, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(dbFilePath, vm, dm);

//        isDatabaseConnected.set(true); // 设置数据库连接状态为已连接
//        isDatabaseConnected=true;
        // 2.通过共享内存在客户端和服务端之间传递数据库连接状态
        //2.1 创建或获取共享内存文件
        if (!SHARED_MEM_FILE.exists()) {
            try {
                SHARED_MEM_FILE.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        RandomAccessFile raf = new RandomAccessFile(SHARED_MEM_FILE, "rw");
        FileChannel channel = raf.getChannel();
        // 将文件映射到共享内存
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
        // 设置数据库连接状态为正在使用
        buffer.put((byte) 1);


        // 3.启动服务端

        new Server(port, tbm).start();

    }

    private static void deleteDB(String path) throws IOException {

        //1.判断是否有客户端连接到该数据库
        //1.1 创建或获取共享内存文件
        RandomAccessFile raf = new RandomAccessFile(SHARED_MEM_FILE, "rw");
        FileChannel channel = raf.getChannel();
        // 将文件映射到共享内存
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
        // 获取数据库连接状态
//        System.out.println(buffer.get());
        byte status = buffer.get();

        if(status==1){
            System.err.println("当前有客户端连接到该数据库，无法删除。");
            return;
        }

        File directory = new File(path);
        if (!directory.exists()) {
            System.err.println("数据库路径不存在: " + path);
            return;
        }

        System.out.println("即将删除数据库路径: " + path + " 下的以下类型文件: bt, db, log, xid");
        System.out.println("请确认是否继续... (Y/N)");
        java.util.Scanner scanner = new java.util.Scanner(System.in);
        String confirmation = scanner.nextLine().toUpperCase();
        if (!"Y".equals(confirmation)) {
            System.out.println("删除操作已取消。");
            return;
        }

        List<String> fileExtensions = Arrays.asList("bt", "db", "log", "xid");
        deleteFiles(directory, fileExtensions);
    }

    private static void deleteFiles(File directory, List<String> fileExtensions) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files!= null) {
                for (File file : files) {
                    if (file.isFile()) {
                        String fileName = file.getName();
                        int lastIndex = fileName.lastIndexOf('.');
                        if (lastIndex > 0) {
                            String extension = fileName.substring(lastIndex + 1);
                            if (fileExtensions.contains(extension)) {
                                System.out.println("正在删除文件: " + file.getAbsolutePath());
                                if (!file.delete()) {
                                    System.err.println("无法删除文件: " + file.getAbsolutePath());
                                }
                            }
                        }
                    } else {
                        deleteFiles(file, fileExtensions); // 递归处理子目录
                    }
                }
            }
        }
    }
    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
