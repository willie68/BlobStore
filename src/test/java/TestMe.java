import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.primitives.Ints;

import de.mcs.blobstore.ChunkEntry;
import de.mcs.blobstore.utils.DefaultIDGenerator;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.ByteArrayUtils;
import de.mcs.utils.GsonUtils;

class TestMe {

  @BeforeEach
  void setUp() throws Exception {
  }

  @Test
  void test() {
    byte[] bytes = new byte[32];
    new Random().nextBytes(bytes);
    // assertEquals(HashUtils.bytesAsHexString(bytes).toLowerCase(),
    // HashUtils.encodeHexString(bytes).toLowerCase());

    for (int x = 0; x < 100; x++) {
      Monitor monOld = MeasureFactory.start("new hex");
      for (int i = 0; i < 100000; i++) {
        String hexString = ByteArrayUtils.bytesAsHexString(bytes);
      }
      monOld.stop();

    }
    System.out.println(MeasureFactory.asString());
  }

  @Test
  void test2() {
    System.out.println("@@@@".getBytes(StandardCharsets.UTF_8).length);

    ByteBuffer buf = ByteBuffer.allocate(16);
    long leastSignificantBits = UUID.randomUUID().getLeastSignificantBits();
    long mostSignificantBits = UUID.randomUUID().getMostSignificantBits();
    buf.putLong(mostSignificantBits);
    buf.putLong(leastSignificantBits);
    System.out.println(buf.array().length);
    System.out.println(Ints.toByteArray(1).length);
  }

  @Test
  void testRnd() {
    byte[] buf1 = new byte[1024 * 1024];
    new Random(1234).nextBytes(buf1);

    byte[] buf2 = new byte[1024 * 1024];
    new Random(1234).nextBytes(buf2);

    assertTrue(Arrays.equals(buf1, buf2));
  }

  enum STATUS {
    CREATED, DELETED
  }

  class Item {
    String name;
    private int state;

    /**
     * @return the state
     */
    public STATUS getState() {
      return STATUS.values()[state];
    }

    /**
     * @param state
     *          the state to set
     */
    public void setState(STATUS state) {
      this.state = state.ordinal();
    }

  }

  @Test
  void testJsonEnum() {
    Item item = new Item();
    item.setState(STATUS.DELETED);
    item.name = "Willie";

    System.out.println(GsonUtils.getJsonMapper().toJson(item));
  }

  private static final byte[] KEY_INFIX = "_c".getBytes(StandardCharsets.UTF_8);

  @Test
  void testByteBuffer() {
    byte[] byteID = new DefaultIDGenerator().getByteID();
    String idStr = ByteArrayUtils.bytesAsHexString(byteID);

    ChunkEntry chunkEntry = new ChunkEntry().setChunkNumber(12).setKey(idStr).setContainerName("vlog_0001.vlog")
        .setHash("12737814587154kasfk9364").setStart(3461926916L).setStartBinary(3216596596L).setLength(4875894589L);
    String json = chunkEntry.toJsonString();

    ByteBuffer newKeyBuffer = ByteBuffer.allocate(1024);
    newKeyBuffer.put(byteID);
    newKeyBuffer.put(KEY_INFIX);
    newKeyBuffer.putInt(chunkEntry.getChunkNumber());
    newKeyBuffer.flip();

    byte[] newKey = new byte[newKeyBuffer.limit()];
    newKeyBuffer.get(newKey);

    System.out.println(ByteArrayUtils.bytesAsHexString(byteID));
    System.out.println(ByteArrayUtils.bytesAsHexString(newKey));
  }
}
