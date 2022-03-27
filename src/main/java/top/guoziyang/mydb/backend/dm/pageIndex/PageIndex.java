package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

/**
 * 页面索引 缓存了每一页的空闲空间。用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 * MYDB 用一个比较粗略的算法实现了页面索引，将一页的空间划分成了 40 个区间。
 * 在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中。
 * insert 在请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求
 */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;//该页的空闲区间个数
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 选取一个合适的页面
     * 被选中的页面 会被从PageIndex中移除  表面同一个页面是不允许并发写的 。在上层模块使用完这个页面后 还需要从心插入
     * @param spaceSize 所需要的空闲空间
     * @return
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            //需要多少空间 向上取整
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;
            while (number <= INTERVALS_NO) {
                if (lists[number].size() == 0) { //如果该区间数量没有对应的页面 继续向上查找
                    number++;
                    continue;
                }
                return lists[number].remove(0);  //返回找到的页面信息  并从该记录中删除
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
