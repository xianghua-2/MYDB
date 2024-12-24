package top.xianghua.mydb.server.tbm;

import top.xianghua.mydb.server.dm.DataManager;
import top.xianghua.mydb.server.parser.statement.Begin;
import top.xianghua.mydb.server.parser.statement.Create;
import top.xianghua.mydb.server.parser.statement.Delete;
import top.xianghua.mydb.server.parser.statement.Insert;
import top.xianghua.mydb.server.parser.statement.Select;
import top.xianghua.mydb.server.parser.statement.Update;
import top.xianghua.mydb.server.utils.Parser;
import top.xianghua.mydb.server.vm.VersionManager;

public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
