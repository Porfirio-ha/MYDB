package top.guoziyang.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

/**
 * 抽象页面的具体实现
 * 将默认数据页大小定为 8K。
 * 注意这个页面是存储在内存中的，与已经持久化到磁盘的抽象页面有区别。(就是将实际数据读到通道里面)
 * //todo 表示将持久化的页面数据读取到该缓存页面(内存中的)？？
 */
public class PageImpl implements Page {
    /**
     * 页号 从已开始
     */
    private int pageNumber;
    /**
     * 页面中存储的实际字节数据
     */
    private byte[] data;
    /**
     * 标志着该页面是否位脏页面 在缓存驱逐的时候，脏页面需要被写回磁盘：表示已经被修改
     */
    private boolean dirty;
    private Lock lock;
    /**
     * 用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作
     */
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    //释放当前页面的缓存
    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
