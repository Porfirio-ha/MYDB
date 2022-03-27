package top.guoziyang.mydb.backend.dm.pageIndex;

public class PageInfo {
    //页号
    public int pgno;
    //空闲区间大小
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
