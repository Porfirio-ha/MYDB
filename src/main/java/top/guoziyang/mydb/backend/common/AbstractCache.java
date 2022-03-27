package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    //    map[id:值]
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    /**
     * 只是占位作用  存在 这说明当前数据正在被加载到缓存 不存在：缓存中可能有也可能没右需要再判断
     */
    private HashMap<Long, Boolean> getting;

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 请求的资源正在被加载到缓存中
                lock.unlock();
                try {
                    Thread.sleep(1);  //休眠继续
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;  //跳出循环直到该资源没被使用
            }
            //资源已被加载 且缓存中存在  获取返回资源
            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源  判断缓存是不是满了
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);   //获得需要加载到缓存的数据
        } catch (Exception e) {
            //加载失败 缓存数量回退 资源获取状态回退
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //获得数据成功  更新相关状态
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj); //回源 写回数据库
                references.remove(key);//缓存计数中删除
                cache.remove(key);//从缓存中移除该数据
                count--; //缓存计数更新
            } else {
                references.put(key, ref);  //更新当前缓存的引用计数
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为  等同于添加进缓存
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
