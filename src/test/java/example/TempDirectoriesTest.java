package example;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class TempDirectoriesTest {
  private static final String HOST = "localhost";

  private EmbeddedZookeeper embeddedZookeeper;
  private KafkaServer kafkaServer;
  private int port;

  @Before
  public void setUpTest() {
    embeddedZookeeper = new EmbeddedZookeeper();
    final Properties brokerConfig = TestUtils.createBrokerConfig(0, HOST + ':' + embeddedZookeeper.port(), false,
        true, 0,
        Option.apply(null),
        Option.apply(null),
        Option.apply(null),
        true, false, 0, false, 0, false, 0, Option.apply(null), 1, false);
    kafkaServer = TestUtils.createServer(new KafkaConfig(brokerConfig), Time.SYSTEM);
    port = TestUtils.boundPort(kafkaServer, SecurityProtocol.PLAINTEXT);
  }

  @Test
  public void writeAStringInATopic() {
    final Map<String, Object> config = new HashMap<>();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HOST + ':' + port);
    try (final KafkaProducer<String, String> producer =
             new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
      producer.send(new ProducerRecord<>("qwerty", "whatever"));
    }
  }

  @After
  public void tearDownTest() {
    try {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();

      embeddedZookeeper.shutdown();
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
}