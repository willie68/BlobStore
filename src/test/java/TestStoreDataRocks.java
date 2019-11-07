import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.Files;

public class TestStoreDataRocks {

  private static final String TEST_KEY = "test";
  private static final int MAX_VALUES = 10000000;
  private static final int MAX_BLB_VALUES = 1000;
  private static final int MAX_FAMILIES = 1000;

  @BeforeClass
  public static void beforeClass() throws Exception {
    RocksDB.loadLibrary();
  }

  public RocksDB db;
  private List<ColumnFamilyDescriptor> cfDescriptors;
  private List<ColumnFamilyHandle> columnFamilyHandleList;

  @Before
  public void before() throws RocksDBException, IOException {
    File folder = new File("./mydb");
    Files.remove(folder, true);
    folder.mkdirs();
    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {

      cfDescriptors = new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));

      columnFamilyHandleList = new ArrayList<>();
      Options options = new Options().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
      List<byte[]> listColumnFamilies = RocksDB.listColumnFamilies(options, "./mydb");
      for (byte[] bs : listColumnFamilies) {
        String name = new String(bs);
        System.out.printf("adding family: %s\r\n", name);
        cfDescriptors.add(new ColumnFamilyDescriptor(bs, cfOpts));
      }

      try (final DBOptions dboptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
        db = RocksDB.open(dboptions, "./mydb", cfDescriptors, columnFamilyHandleList);
        db.compactRange();
      } catch (RocksDBException e) {
        throw e;
      }
    }
  }

  @After
  public void after() {
    if (db != null) {
      for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
        columnFamilyHandle.close();
      }
      db.close();
    }
  }

  @Test
  public void test() throws RocksDBException {
    System.out.println("write");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      Monitor m = MeasureFactory.start("put");
      try {
        dbPut(null, String.format("test%d", i), String.format("bar%d", i));
      } finally {
        m.stop();
      }
    }

    System.out.println();
    System.out.println("read");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      String value;
      Monitor m = MeasureFactory.start("get");
      try {
        value = dbGet(null, String.format("test%d", i));
      } finally {
        m.stop();
      }
      String name = String.format("bar%d", i);
      assertEquals(name, value);
    }

    System.out.println();
    System.out.println("random");
    Random rnd = new Random();
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      String value;
      int pos = rnd.nextInt(MAX_VALUES) + 1;
      Monitor m = MeasureFactory.start("rnd");
      try {
        value = dbGet(null, String.format("test%d", pos));
      } finally {
        m.stop();
      }
      String name = String.format("bar%d", pos);
      assertEquals(name, value);
    }

    System.out.println();
    System.out.println("delete");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      Monitor m = MeasureFactory.start("del");
      try {
        dbDel(null, String.format("test%d", i));
      } finally {
        m.stop();
      }
    }
    System.out.println(MeasureFactory.asString());
  }

  private void progress(long i, long max) {
    if ((i % (max / 1000)) == 0) {
      System.out.print(".");
    }
    if ((i % (max / 10)) == 0) {
      System.out.println();
    }
  }

  private void dbPut(ColumnFamilyHandle columnFamilyHandle, String key, String value) throws RocksDBException {
    if (columnFamilyHandle == null) {
      db.put(key.getBytes(), value.getBytes());
    } else {
      db.put(columnFamilyHandle, key.getBytes(), value.getBytes());
    }
  }

  private String dbGet(ColumnFamilyHandle columnFamilyHandle, String key) throws RocksDBException {
    byte[] bs = null;
    if (columnFamilyHandle == null) {
      bs = db.get(key.getBytes());
    } else {
      bs = db.get(columnFamilyHandle, key.getBytes());
    }
    if (bs != null) {
      return new String(bs);
    }
    return null;
  }

  private void dbDel(ColumnFamilyHandle columnFamilyHandle, String key) throws RocksDBException {
    if (columnFamilyHandle == null) {
      db.delete(key.getBytes());
    } else {
      db.delete(columnFamilyHandle, key.getBytes());
    }
  }

  @Test
  public void testFamilySimple() throws RocksDBException {
    ColumnFamilyHandle columnFamilyHandle = null;
    {
      System.out.println("create family");
      ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
      Monitor m = MeasureFactory.start("createFamily");
      try {
        columnFamilyHandle = db.createColumnFamily(new ColumnFamilyDescriptor(getFamilyName(10), cfOpts));
        columnFamilyHandleList.add(columnFamilyHandle);
      } finally {
        m.stop();
      }
    }

    String value = "bar";
    {
      System.out.println();
      System.out.println("write");
      Monitor m = MeasureFactory.start("put");
      try {
        dbPut(columnFamilyHandle, TEST_KEY, value);
      } finally {
        m.stop();
      }
    }
    {
      System.out.println();
      System.out.println("read");

      String myValue;
      Monitor m = MeasureFactory.start("get");
      try {
        myValue = dbGet(columnFamilyHandle, TEST_KEY);
      } finally {
        m.stop();
      }
      assertEquals(value, myValue);
    }

    {
      System.out.println();
      System.out.println("delete");
      Monitor m = MeasureFactory.start("del");
      try {
        dbDel(columnFamilyHandle, TEST_KEY);
      } finally {
        m.stop();
      }
      assertNull(dbGet(columnFamilyHandle, TEST_KEY));
    }
    System.out.println(MeasureFactory.asString());
  }

  @Test
  public void testFamilyPerformance() throws RocksDBException {
    System.out.println("create families");
    ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
    for (int i = 0; i < MAX_FAMILIES; i++) {
      if ((i % 10) == 0) {
        System.out.print(".");
      }
      if ((i % 100) == 0) {
        System.out.println();
      }
      Monitor m = MeasureFactory.start("createFamily");
      try {
        ColumnFamilyHandle columnFamilyHandle = db
            .createColumnFamily(new ColumnFamilyDescriptor(getFamilyName(i), cfOpts));
        columnFamilyHandleList.add(columnFamilyHandle);
      } finally {
        m.stop();
      }
    }

    System.out.println("write");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleList.get(i % columnFamilyHandleList.size());
      Monitor m = MeasureFactory.start("put");
      try {
        dbPut(columnFamilyHandle, String.format("test%d", i), String.format("bar%d", i));
      } finally {
        m.stop();
      }
    }

    System.out.println();
    System.out.println("read");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleList.get(i % columnFamilyHandleList.size());
      String value;
      Monitor m = MeasureFactory.start("get");
      try {
        value = dbGet(columnFamilyHandle, String.format("test%d", i));
      } finally {
        m.stop();
      }
      String name = String.format("bar%d", i);
      assertEquals(name, value);
    }

    System.out.println();
    System.out.println("random");
    Random rnd = new Random();
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      String value;
      int pos = rnd.nextInt(MAX_VALUES) + 1;
      ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleList.get(pos % columnFamilyHandleList.size());
      Monitor m = MeasureFactory.start("rnd");
      try {
        value = dbGet(columnFamilyHandle, String.format("test%d", pos));
      } finally {
        m.stop();
      }
      String name = String.format("bar%d", pos);
      assertEquals(name, value);
    }

    System.out.println();
    System.out.println("delete");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      ColumnFamilyHandle columnFamilyHandle = columnFamilyHandleList.get(i % columnFamilyHandleList.size());
      Monitor m = MeasureFactory.start("del");
      try {
        dbDel(columnFamilyHandle, String.format("test%d", i));
      } finally {
        m.stop();
      }
    }
    System.out.println(MeasureFactory.asString());
  }

  private byte[] getFamilyName(int i) {
    String name = String.format("family_%d", i);
    return name.getBytes();
  }

  // @Test
  // public void testBigBinary() throws RocksDBException {
  // dbPut("foo", "bar");
  // assertEquals("bar", dbGet("foo"));
  // byte[] buffer = new byte[10 * 1024 * 1024];
  // Random rnd = new Random();
  // rnd.nextBytes(buffer);
  // System.out.println("blb-write");
  // for (int i = 1; i <= MAX_BLB_VALUES; i++) {
  // progress(i, MAX_BLB_VALUES);
  // Monitor m = MeasureFactory.start("blb-put");
  // try {
  // dbBlbPut(String.format("test%d", i), buffer);
  // } finally {
  // m.stop();
  // }
  // }
  //
  // System.out.println();
  // System.out.println("blb-read");
  // for (int i = 1; i <= MAX_BLB_VALUES; i++) {
  // progress(i, MAX_BLB_VALUES);
  // byte[] value;
  // Monitor m = MeasureFactory.start("blb-get");
  // try {
  // value = dbBlbGet(String.format("test%d", i));
  // } finally {
  // m.stop();
  // }
  // String name = String.format("bar%d", i);
  // assertEquals(buffer, value);
  // }
  //
  // System.out.println();
  // System.out.println("blb-random");
  // for (int i = 1; i <= MAX_BLB_VALUES / 1000; i++) {
  // progress(i, MAX_BLB_VALUES);
  // int pos = rnd.nextInt(MAX_BLB_VALUES) + 1;
  // byte[] value;
  // Monitor m = MeasureFactory.start("rnd");
  // try {
  // value = dbBlbGet(String.format("test%d", pos));
  // } finally {
  // m.stop();
  // }
  // assertEquals(buffer, value);
  // }
  //
  // System.out.println();
  // System.out.println("delete");
  // for (int i = 1; i <= MAX_BLB_VALUES; i++) {
  // progress(i, MAX_BLB_VALUES);
  // Monitor m = MeasureFactory.start("del");
  // try {
  // dbBlbDel(String.format("test%d", i));
  // } finally {
  // m.stop();
  // }
  // }
  // System.out.println(MeasureFactory.asString());
  // }
  //
  // private void dbBlbPut(String key, byte[] value) throws RocksDBException {
  // db.put(key.getBytes(), value);
  // }
  //
  // private byte[] dbBlbGet(String key) throws RocksDBException {
  // return db.get(key.getBytes());
  // }
  //
  // private void dbBlbDel(String key) throws RocksDBException {
  // db.delete(key.getBytes());
  // }
}
