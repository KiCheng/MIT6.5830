package simpledb.storage;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

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
        this.lruCache = new LRUCache(this.numPages);
        // 添加锁管理
        this.lockManager = new LockManager();
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
            this.capacity = capacity;  // LRU链表容量
            this.size = 0;  // 当前LRU链表大小
            map = new ConcurrentHashMap<>();  // 用于存储PageId和Node的映射
            head.next = tail;  // 初始化头节点
            tail.prev = head;  // 初始化尾节点
        }

        // debug 解决pageId对象比较的问题
        public synchronized boolean contain(PageId key) {
            for (PageId pageId: map.keySet()) {
                if (pageId.equals(key)) {
                    return true;
                }
            }
            return false;
        }


        public synchronized Page get(PageId key) {
//            if (map.containsKey(key)) {
            if (contain(key)) {
                Node node = map.get(key);
                // 定位到链表头
                moveToHead(node);
                return node.val;
            } else {
                return null;
            }
        }

        public synchronized void put(PageId key, Page val) throws DbException {
            if (contain(key)) {
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
                    // 移除链表尾(我们可以通过丢弃脏页并从磁盘重新读取来进行事务的回滚，因为我们不能在LRU中删除脏页)
                    Node removeNode = tail.prev;
                    // 丢弃的不能是脏页，是脏页则跳过
                    while (removeNode.val.isDirty() != null) {
                        removeNode = removeNode.prev;
                        if (removeNode == newNode || removeNode == tail) {
                            throw new DbException("没有合适的页存储空间或者所有页都为脏页！！");
                        }
                    }

                    // 注意不能删除脏页
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

        // 删除尾部节点(但是注意不能删除脏页，所以需要从尾节点开始判断是否为脏页，脏页跳过)
        private Node removeTail() {
            Node node = tail.prev;
            // node为脏页则继续往前找
            while (node.val.isDirty() != null && node != head) {
                node = node.prev;
            }
            if (node == head || node == tail) {
                throw new RuntimeException("没有合适的页空间或者所有页都为脏页!");
            }

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

        // 传入的key是new出来的，所以需要和PageId进行等值判断
        public synchronized Node getNodeByKey(PageId key) {
            for (PageId pageId: map.keySet()) {
                if (pageId.equals(key)) {
                    return map.get(pageId);
                }
            }
            return null;
        }

        // 根据pageId删除页
        public synchronized void removeByKey(PageId key) {
            Node node = getNodeByKey(key);
            if (node != null) {
                removeNode(node);
                map.remove(key);
            } else {
                throw new RuntimeException("「LRU缓存」：删除的节点不存在!");
            }
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
        LockType lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockType.SHARED_LOCK;
        } else {
            lockType = LockType.EXCLUSIVE_LOCK;
        }
        // 如果获取lock失败(reTry 3次)则直接放弃事务
        try {
            if (!lockManager.acquireLock(pid, tid, lockType, 0)) {
                // 获取锁失败，回滚事务
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Method 「getPage」获取锁发生异常！！！");
        }

        // bufferPool应直接放在直接内存
        if (lruCache.get(pid) == null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);  // 调用HeapFile的readPage方法
            lruCache.put(pid, page);
        }
        return lruCache.get(pid);
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
    private LockManager lockManager = new LockManager();

    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
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
        // 1、事务成功则刷新页面到磁盘，事务失败则回滚将磁盘反向刷新到BufferPool
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            rollBack(tid);
        }
        // 2、释放tid事务上所有的锁
        lockManager.releasePageByTid(tid);
    }


    public synchronized void rollBack(TransactionId tid) {
        for(Map.Entry<PageId, LRUCache.Node> group: lruCache.getEntrySet()) {
            PageId pageId = group.getKey();
            Page page = group.getValue().val;
            if (tid.equals(page.isDirty())) {
                int tableId = pageId.getTableId();
                DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                Page readPage = table.readPage(pageId);
                // 将磁盘的页面反向刷新至BufferPool完成事务回滚
                lruCache.removeByKey(pageId);
//                lruCache.put(pageId, readPage);
            }
        }
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

    public void updateBufferPool(List<Page> updatePages, TransactionId id) throws DbException {
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

        for (Map.Entry<PageId, LRUCache.Node> group: lruCache.getEntrySet()) {
            PageId pageId = group.getKey();
            Page flushPage = group.getValue().val;
            TransactionId dirtyTid = flushPage.isDirty();
            Page before = flushPage.getBeforeImage();
            // 涉及到事务提交就应该setBeforeImage，更新数据，方便后续事务终止能够回退到该版本
            if (dirtyTid != null && dirtyTid.equals(tid)) {
                Database.getLogFile().logWrite(tid, before, flushPage);
                Database.getCatalog().getDatabaseFile(pageId.getTableId()).writePage(flushPage);
            }
        }
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

    /**
     * 加锁
     */
    @Data
    private class PageLock {
        private TransactionId tid;
        private PageId pid;
        private LockType type;

        public PageLock(TransactionId tid, PageId pid, LockType type) {
            this.tid = tid;
            this.pid = pid;
            this.type = type;
        }

        public LockType getType() {
            return type;
        }
    }

    /**
     * 锁类型枚举类
     */
    public enum LockType {
        SHARED_LOCK (0, "共享锁"),
        EXCLUSIVE_LOCK (1, "排它锁");

        @Getter
        private Integer code;
        @Getter
        private String value;

        LockType(int code, String value) {
            this.code = code;
            this.value = value;
        }
    }

    private class LockManager {
        @Getter
        public ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

        public LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        /**
         * Return true if the specified transaction has a lock on the specified page
         */
        public boolean holdsLock(TransactionId tid, PageId p) {
            // TODO: some code goes here
            // not necessary for lab1|lab2
            if (lockMap.get(p) == null) {
                return false;
            }
            return lockMap.get(p).get(tid) != null;
        }

        public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType requestLock, int reTry) throws InterruptedException {
            // 重传达到3次
            if (reTry == 3) {
                return false;
            }
            // 用于打印log
            final String thread = Thread.currentThread().getName();
            // 1 页面上不存在锁
            if (lockMap.get(pageId) == null) {
                return putLock(tid, pageId, requestLock);  // 直接获取锁
            }

            // 2 页面上存在锁
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);
            if (tidLocksMap.get(tid) == null) {
                // 2.1 页面上的锁没有自己的
                if (requestLock == LockType.EXCLUSIVE_LOCK) {
                    // 请求的为X锁
                    wait(10);
                    return acquireLock(pageId, tid, requestLock, reTry + 1);  // 不能获取页面上的X锁，等待一段时间后重试
                } else if (requestLock == LockType.SHARED_LOCK) {
                    // 页面上的锁是否都是S锁：页面上的锁大于1个就都是S锁（因为X锁只能被一个事务占有）
                    if (tidLocksMap.size() > 1) {
                        // 都是读锁直接获取
                        return putLock(tid, pageId, requestLock);
                    } else {
                        Collection<PageLock> values = tidLocksMap.values();
                        // values集合中只有一个元素
                        for (PageLock value: values) {
                            if (value.getType() == LockType.EXCLUSIVE_LOCK) {
                                // 存在的锁为X锁，不能获取页面上的X锁，等待一段时间后重试
                                wait(10);
                                return acquireLock(pageId, tid, requestLock, reTry + 1);
                            } else {
                                // 存在的锁为S锁 -- 直接获取
                                return putLock(tid, pageId, requestLock);
                            }
                        }
                    }
                }
            } else {
                // 2.2 页面上的锁是自己的
                if (requestLock == LockType.SHARED_LOCK) {
                    tidLocksMap.remove(tid);  // 先移除自己的锁
                    return putLock(tid, pageId, requestLock);
                } else {
                    // 判断自己的锁是否是X锁，如果是则直接获取
                    if (tidLocksMap.get(tid).getType() == LockType.EXCLUSIVE_LOCK) {
                        return true;
                    } else {
                        // 拥有的是读锁，判断是否还存在别的锁
                        if (tidLocksMap.size() > 1) {
                            wait(10);
                            return acquireLock(pageId, tid, requestLock, reTry + 1);
                        } else {
                            // 只有自己拥有一个读锁，进行锁升级
                            tidLocksMap.remove(tid);
                            return putLock(tid, pageId, requestLock);
                        }
                    }
                }
            }
            return false;
        }

        public boolean putLock(TransactionId tid, PageId pageId, LockType requestLock) {
            ConcurrentHashMap<TransactionId, PageLock> tidLocksMap = lockMap.get(pageId);
            // 页面上一个锁都没有
            if (tidLocksMap == null) {
                tidLocksMap = new ConcurrentHashMap<>();
                lockMap.put(pageId, tidLocksMap);
            }
            // 获取新锁
            PageLock pageLock = new PageLock(tid, pageId, requestLock);
            tidLocksMap.put(tid, pageLock);
            lockMap.put(pageId, tidLocksMap);
            return true;
        }


        /**
         * 释放某个事务上面所有页的锁
         */
        public synchronized void releasePageByTid(TransactionId tid) {
            Set<PageId> pageIds = lockMap.keySet();
            for (PageId pageId: pageIds) {
                releasePage(tid, pageId);
            }
        }

        /**
         * 释放某个页上tid的锁
         */
        public synchronized void releasePage(TransactionId tid, PageId pid) {
            if (holdsLock(tid, pid)) {
                ConcurrentHashMap<TransactionId, PageLock> tidLocks = lockMap.get(pid);
                tidLocks.remove(tid);
                if (tidLocks.isEmpty()) {  // size=0
                    lockMap.remove(pid);
                }
                // 释放锁时就唤醒正在等待的线程，因为wait和notifyAll必须在同步代码块中使用，所以要加synchronized
                this.notifyAll();
            }
        }
    }


}
