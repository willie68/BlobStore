import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;
import de.mcs.utils.HasherUtils;

class TestMe {

  @BeforeEach
  void setUp() throws Exception {
  }

  @Test
  void test() {
    byte[] bytes = new byte[32];
    new Random().nextBytes(bytes);
    // assertEquals(HasherUtils.bytesAsHexString(bytes).toLowerCase(),
    // HasherUtils.encodeHexString(bytes).toLowerCase());

    for (int x = 0; x < 100; x++) {
      Monitor monOld = MeasureFactory.start("new hex");
      for (int i = 0; i < 100000; i++) {
        String hexString = HasherUtils.bytesAsHexString(bytes);
      }
      monOld.stop();

    }
    System.out.println(MeasureFactory.asString());
  }

  @Test
  void test2() {
    System.out.println("@@@@".getBytes(Charset.forName("UTF-8")).length);

    ByteBuffer buf = ByteBuffer.allocate(15);
    long leastSignificantBits = UUID.randomUUID().getLeastSignificantBits();
    long mostSignificantBits = UUID.randomUUID().getMostSignificantBits();
    buf.putLong(mostSignificantBits);
    buf.putLong(leastSignificantBits);
    System.out.println(buf.array().length);
  }
}
