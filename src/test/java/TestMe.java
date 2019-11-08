import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.mcs.blobstore.utils.HasherUtils;
import de.mcs.jmeasurement.MeasureFactory;
import de.mcs.jmeasurement.Monitor;

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

}
