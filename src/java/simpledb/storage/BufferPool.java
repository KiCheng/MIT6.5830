package simpledb.storage;

import lombok.val;
import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;  // 4KB ???

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

//    private final Map<PageId, Page> bufferPools = new ConcurrentHashMap<>();

    // 将bufferPools的hashMap改为LRUCache数据结构
    private LRUCache lruCache;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        this.numPages = numPages;
        this.lruCache = new LRUCache(numPages);
    }

    /**
     * 为了实现LRU算法，需要维护一个双向链表，用于记录每个PageId的访问顺序
     */
    private static class LRUCache {
        int capacity, size;
        ConcurrentHashMap<PageId, Node> map;
        // 头节点和尾节点：标志位无数据
        Node head = new Node();
        Node tail = new Node();

        public LRUCache(int capacity) {
            this.capacity = capacity;
            this.size = 0;
            map = new ConcurrentHashMap<>();
            head.next = tail;
            tail.prev = head;
        }

        public synchronized Page get(PageId key) {
            if (map.containsKey(key)) {
                Node node = map.get(key);
                // 定位到链表头
                moveToHead(node);
                return node.val;
            } else {
                return null;
            }
        }

        public synchronized void put(PageId key, Page val) {
            if (map.containsKey(key)) {
                // 更新value
                Node node = map.get(key);
                node.val = val;
                moveToHead(node);
            } else {
                Node newNode = new Node(key, val);
                map.put(key, newNode);
                // 添加到链表头
                addToHead(newNode);
                size++;
                if (size > capacity) {
                    // 移除链表尾
                    Node node = removeTail();
                    map.remove(node.key);
                    size--;
                }
            }
        }

        // 添加到链表头部
        private synchronized void addToHead(Node node) {
            node.prev = head;
            node.next = head.next;
            head.next.prev = node;
            head.next = node;
        }

        // 移动到链表头部
        private synchronized void moveToHead(Node node) {
            // 先从原位置删除node
            removeNode(node);
            // 再将node插入链表头部
            addToHead(node);
        }

        // 删除node节点
        private void removeNode(Node node) {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }

        // 删除尾部节点
        private Node removeTail() {
            Node node = tail.prev;
            removeNode(node);
            return node;
        }

        private synchronized int getSize() {
            return size;
        }

        private static class Node {
            PageId key;
            Page val;
            Node prev;
            Node next;

            public Node() {}

            public Node(PageId key, Page val) {
                this.key = key;
                this.val = val;
            }
        }

        public Set<Map.Entry<PageId, Node>> getEntrySet() {
            return map.entrySet();
        }
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (lruCache.get(pid) == null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);  // 调用HeapFile的readPage方法
            lruCache.put(pid, page);
        }
        return lruCache.get(pid);

//        if(!bufferPools.containsKey(pid)) {
//            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = file.readPage(pid);  // 调用HeapFile的readPage方法
//            bufferPools.put(pid, page);
//        }
//        return bufferPools.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> updatePages = f.insertTuple(tid, t);
        updateBufferPool(updatePages, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> updatePages = f.deleteTuple(tid, t);
        updateBufferPool(updatePages, tid);
    }

    public void updateBufferPool(List<Page> updatePages, TransactionId id) {
        for (Page page: updatePages) {
            page.markDirty(true, id);  // 设置为脏页（因为在BufferPool中修改page后，和磁盘中的page不一致了）
            // update BufferPool
            lruCache.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, LRUCache.Node> group: lruCache.getEntrySet()) {
            Page page = group.getValue().val;
            if (page.isDirty() != null) {
                flushPage(group.getKey());  // 将不是脏页的页面刷新页面到磁盘
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        if (pid != null) {
            lruCache.map.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        Page page = lruCache.get(pid);
        if (page == null) {
            return;
        }
        TransactionId tid = page.isDirty();
        if (tid != null) {
            Page before = page.getBeforeImage();
            Database.getLogFile().logWrite(tid, before, page);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        /**
         * 直接在LRUCache中实现LRU算法
         */
    }

}
