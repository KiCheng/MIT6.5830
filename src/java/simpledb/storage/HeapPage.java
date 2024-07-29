package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;

    /**
     * 表示tuple状态的比特位图
     */
    final byte[] header;
    final Tuple[] tuples;

    /**
     * 每个页面中的槽的数目
     */
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * 脏页标记
     */
    private boolean dirtyFlag;

    /**
     * 产生脏页的事务id
     */
    private TransactionId dirtyTid;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // TODO: some code goes here
        int page_bits = BufferPool.getPageSize() * 8;
        int tuple_bits = td.getSize() * 8 + 1;
        return page_bits / tuple_bits;  // int/int=int
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // TODO: some code goes here
        return (int) Math.ceil((double) getNumTuples() / 8);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // TODO: some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len];  //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // TODO: some code goes here
        int tid = t.getRecordId().getTupleNumber();
        boolean flag = false;

        for (int i = 0; i < tuples.length; i++) {
            if(tuples[i] != null && compareTuplesByFields(t, tuples[i])) {
                if (!isSlotUsed(i)) {
                    throw new DbException("tuple slot is already empty");
                }
                markSlotUsed(i, false);
                tuples[tid] = null;  // delete
                flag = true;
            }
        }
        if (!flag) {
            throw new DbException("the tuple is not on this page");
        }
    }

    /**
     * 比较两个tuple的字段是否相等
     * @param t1
     * @param t2
     * @return
     */
    private boolean compareTuplesByFields(Tuple t1, Tuple t2) {
        if (t1.getTupleDesc().equals(t2.getTupleDesc())) {
            for (int j = 0; j < t1.getTupleDesc().numFields(); j++) {
                if (!t1.getField(j).equals(t2.getField(j))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        if (getNumUnusedSlots() == 0 || !t.getTupleDesc().equals(td)) {
            throw new DbException("page is full or tupleDesc is mismatch.");
        }
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                markSlotUsed(i, true);
                t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1
        this.dirtyFlag = dirty;
        this.dirtyTid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // TODO: some code goes here
        // Not necessary for lab1
        return dirtyFlag ? dirtyTid : null;
    }

    /**
     * Returns the number of unused (i.e., empty) slots on this page.
     */
    public int getNumUnusedSlots() {
        // TODO: some code goes here
        int cnt = 0;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                cnt++;
            }
        }
        return cnt;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // TODO: some code goes here
        // 计算在header中的位置(哪一个字节)
        int iTh = i / 8;
        // 计算具体在bitmap中的位置(哪一个比特位，或者理解为header字节位中的比特偏移量)
        int bitTh = i % 8;
        int onBit = (header[iTh] >> bitTh) & 1;
        return onBit == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // TODO: some code goes here
        // not necessary for lab1
        int iTh = i / 8;
        int bitTh = i % 8;
        int onBit = (header[iTh] >> bitTh) & 1;  // 判断该slot位是否被使用
        if (onBit == 0 && value) {
            // 未使用，现在使用
            header[iTh] += (byte) (1 << bitTh);
        } else if (onBit == 1 && !value) {
            // 已使用，现在未使用
            header[iTh] -= (byte) (1 << bitTh);
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // TODO: some code goes here
        List<Tuple> tupleList = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i)) {
                tupleList.add(tuples[i]);
            }
        }
        return tupleList.iterator();
    }

}

