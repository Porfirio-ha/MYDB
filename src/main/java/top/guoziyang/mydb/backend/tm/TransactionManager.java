package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

/**
 * 事务管理器
 * 三种状态：1 active 正在进行
 * 2.commited 已提交
 * 3， aborted 已撤销(回滚)
 */
public interface TransactionManager {
    long begin(); //开启一个新事务

    void commit(long xid); // 提交一个事务

    void abort(long xid); // 取消一个事务

    boolean isActive(long xid);// 查询一个事务的状态是否是正在进行的状态

    boolean isCommitted(long xid); // 查询一个事务的状态是否是已提交

    boolean isAborted(long xid); // 查询一个事务的状态是否是已取消

    void close();  //关闭事务

    /**
     * 创建一个事务管理器的实现类
     *
     * @param path 文件路径
     * @return
     */
    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {//创建.XID文件
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        //判断是否可读可写
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开一个XID文件
     *
     * @param path 路径
     * @return TransactionManagerImpl 事务管理器
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {//判断是否存在
            Panic.panic(Error.FileNotExistsException);
        }
        //判断是否可读可写
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
