package simpledb.common;

import simpledb.storage.DbFile;


public class Table {
    /**
     * 要添加的table内容
     */
    private DbFile file;

    /**
     * table名
     */
    private String name;

    /**
     * table主键字段的名称
     */
    private String pkeyField;

    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public Table(DbFile file, String name) {
        this(file, name, "");
    }

    public DbFile getFile() {
        return file;
    }

    public void setFile(DbFile file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }
}
