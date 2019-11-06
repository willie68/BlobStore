import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;

public class TestStoreDataRocks {

  private static final int MAX_VALUES = 10000000;
  private static final int MAX_BLB_VALUES = 1000;

  @BeforeClass
  public static void beforeClass() throws Exception {
    RocksDB.loadLibrary();
  }

  public RocksDB db;

  @Before
  public void before() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true)) {
      db = RocksDB.open(options, "./mydb");
      db.compactRange();
    } catch (RocksDBException e) {
      throw e;
    }
  }

  @After
  public void after() {
    if (db != null) {
      db.close();
    }
  }

  @Test
  public void test() throws RocksDBException {
    dbPut("foo", "bar");
    assertEquals("bar", dbGet("foo"));

    System.out.println("write");
    for (int i = 1; i <= MAX_VALUES; i++) {
      progress(i, MAX_VALUES);
      Monitor m = MeasureFactory.start("put");
      try {
        dbPut(String.format("test%d", i), String.format("bar%d", i));
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
        value = dbGet(String.format("test%d", i));
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
        value = dbGet(String.format("test%d", pos));
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
        dbDel(String.format("test%d", i));
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

  private void dbPut(String key, String value) throws RocksDBException {
    db.put(key.getBytes(), value.getBytes());
  }

  private String dbGet(String key) throws RocksDBException {
    byte[] bs = db.get(key.getBytes());
    if (bs != null) {
      return new String(bs);
    }
    return null;
  }

  private void dbDel(String key) throws RocksDBException {
    db.delete(key.getBytes());
  }

  @Test
  public void testBigBinary() throws RocksDBException {
    dbPut("foo", "bar");
    assertEquals("bar", dbGet("foo"));
    byte[] buffer = new byte[10 * 1024 * 1024];
    Random rnd = new Random();
    rnd.nextBytes(buffer);
    System.out.println("blb-write");
    for (int i = 1; i <= MAX_BLB_VALUES; i++) {
      progress(i, MAX_BLB_VALUES);
      Monitor m = MeasureFactory.start("blb-put");
      try {
        dbBlbPut(String.format("test%d", i), buffer);
      } finally {
        m.stop();
      }
    }

    System.out.println();
    System.out.println("blb-read");
    for (int i = 1; i <= MAX_BLB_VALUES; i++) {
      progress(i, MAX_BLB_VALUES);
      byte[] value;
      Monitor m = MeasureFactory.start("blb-get");
      try {
        value = dbBlbGet(String.format("test%d", i));
      } finally {
        m.stop();
      }
      String name = String.format("bar%d", i);
      assertEquals(buffer, value);
    }

    System.out.println();
    System.out.println("blb-random");
    for (int i = 1; i <= MAX_BLB_VALUES / 1000; i++) {
      progress(i, MAX_BLB_VALUES);
      int pos = rnd.nextInt(MAX_BLB_VALUES) + 1;
      byte[] value;
      Monitor m = MeasureFactory.start("rnd");
      try {
        value = dbBlbGet(String.format("test%d", pos));
      } finally {
        m.stop();
      }
      assertEquals(buffer, value);
    }

    System.out.println();
    System.out.println("delete");
    for (int i = 1; i <= MAX_BLB_VALUES; i++) {
      progress(i, MAX_BLB_VALUES);
      Monitor m = MeasureFactory.start("del");
      try {
        dbBlbDel(String.format("test%d", i));
      } finally {
        m.stop();
      }
    }
    System.out.println(MeasureFactory.asString());
  }

  private void dbBlbPut(String key, byte[] value) throws RocksDBException {
    db.put(key.getBytes(), value);
  }

  private byte[] dbBlbGet(String key) throws RocksDBException {
    return db.get(key.getBytes());
  }

  private void dbBlbDel(String key) throws RocksDBException {
    db.delete(key.getBytes());
  }
}
