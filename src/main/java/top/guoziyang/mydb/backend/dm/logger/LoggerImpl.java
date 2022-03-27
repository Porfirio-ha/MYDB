package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 * <p>
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的校验和，int类型
 * BadTail 是在数据库崩溃时，没有来的及写完的数据日志  （不一定存在）
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度 0-4 对应于 OF_Size-OF_CheckSum 的位置
 * Checksum 4字节int  对应于OF_checksUM-ofDATE的位置
 * Date 不定长    由ofDATE和size确定长度
 */
public class LoggerImpl implements Logger {
    //生成校验和的种子
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;//4
    private static final int OF_DATA = OF_CHECKSUM + 4;//数据存储位置相对指针

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    // 打开以存在日志文件的初始化
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {//说明字节数小于总校验和的长度不符合标准
            Panic.panic(Error.BadLogFileException);
        }
        //1.读取总校验和
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;
        //2.校验日志合法性去除结尾可能存在的不合格数据
        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        //1.初始化读取指针的位置
        rewind();

        int xCheck = 0;
        while (true) {
            //读取下一条日志的全部数据
            byte[] log = internNext();
            if (log == null) break;
            //计算当前日志对于总校验和的叠加影响
            // todo 重复计算了 再获得log时内部已经计算过了呀
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }
        //去除尾部不合规数据
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //重新设置下一次日志文件的写入指针位置
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 计算单条日志的校验和
     *
     * @param xCheck 当前校验和
     * @param log    日志数据
     * @return 校验和与种子的乘积加上当前数据 最后累加得到
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    //写入一条日志数据
    @Override
    public void log(byte[] data) {
        //1.将数据包装 添加[size] 和[checkSum]
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);//写入通道 并没有写入实体文件
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);//更新头部总校验和
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false); //刷新缓冲区 所有内容写入实体文件
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获得下一条日志数据
     *
     * @return
     */
    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        //1. 读取下一条日志的前4各字节 确定长度
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        //2.校验 当前日志长度是否超过文件长度
        if (position + size + OF_DATA > fileSize) {
            return null;
        }
        //3.读取 一条完整的日志 [size][checkSum][date]
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        // 4.校验和对比
        //4.1 计算校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        //4.2 文件中读取校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        //5. 文件指针前移
        position += log.length;
        return log;
    }

    /**
     * 读取下一条日志
     *
     * @return 返回的是实际数据  不包含[size][checkSum]
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 初始化指针位置
     */
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
