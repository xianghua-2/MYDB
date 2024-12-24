package top.xianghua.mydb.server.utils;

public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
