package top.guoziyang.mydb.backend.utils;

/**
 * 打印错误信息并退出
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
