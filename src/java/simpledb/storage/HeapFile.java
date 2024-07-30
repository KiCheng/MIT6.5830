package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    /**
     * 堆文件
     */
    private final File f;

    /**
     * 文件描述
     */
    private final TupleDesc td;

    /**
     * 迭代器内部类
     */
    private static class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private final HeapFile heapFile;

        /**
         * 元组迭代器
         */
        private Iterator<Tuple> tupleIterator;
        /**
         * 当前页面索引
         */
        private int index;

        public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
            this.tid = tid;
            this.heapFile = heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            tupleIterator = getTupleIterator(index);
        }

        private Iterator<Tuple> getTupleIterator(int pageNumber) throws TransactionAbortedException, DbException {
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId heapPageId = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            } else {
                throw new DbException(String.format("heapFile %d does not exist in page %d", heapFile.getId(), pageNumber));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }
            while(!tupleIterator.hasNext()) {
                // 如果当前页面没有元组，则切换到下一个页面
                index++;
                if(index < heapFile.numPages()) {
                    tupleIterator = getTupleIterator(index);
                } else {
                    return false;
                }
            }
            // 如果当前页面还有元组，则返回true
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tupleIterator == null || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
        public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        int offset = pgNo * BufferPool.getPageSize();

        // randomaccessfile try-catch-finally
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(f, "r");
            // 保证pid页面的结束位置不大于文件randomFile的长度
            if ((long) pgNo * BufferPool.getPageSize() > randomAccessFile.length()) {
                randomAccessFile.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            // 移动偏移量到页面的开头
            randomAccessFile.seek(offset);
            // 不要在调用时将整个表加载到内存中，这将导致非常大的表出现内存不足错误
            int read = randomAccessFile.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes not equal to BufferPool.getPageSize()",
                        tableId, pgNo, read));
            }
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(heapPageId, bytes);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.seek(offset);
        file.write(pageData);
        file.close();

        // 标记脏页
        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        long file_length = getFile().length();  // 文件长度
        int pageSize = BufferPool.getPageSize();  // 每个页面的大小
        return (int) (file_length / pageSize);
    }

    // see DbFile.java for javadocs

    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),
                    Permissions.READ_WRITE);
            if (p.getNumUnusedSlots() == 0) {
                // 当前页面没有空闲槽位
                continue;
            }
            p.insertTuple(t);  // 调用HeapPage的insertTuple方法
            pageList.add(p);
            return pageList;
        }
        // 没有找到合适的页面，需要创建新的页面
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bos.write(emptyData);
        bos.close();
        // 加载入BufferPool
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()-1),
                Permissions.READ_WRITE);  // pgNo: numPages()方法的返回值是基于文件当前的大小动态计算的
        p.insertTuple(t);
        pageList.add(p);
        return pageList;
    }
    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        return Collections.singletonList(p);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(tid, this);
    }
}

