package top.guoziyang.mydb.backend.common;

//实现共享内存的数组  即对数组切片：记录下头尾指针  实现类似与go的数组切片后依然共享同一各内存
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
