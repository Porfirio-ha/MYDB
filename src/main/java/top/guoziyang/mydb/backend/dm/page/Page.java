package top.guoziyang.mydb.backend.dm.page;
/**
 * 将文件系统抽象成页面
 * 每一次对文件系统的读写都是以页面为单位的
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
