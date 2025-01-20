package at.ac.uibk.dps.streamprocessingapplications.shared.sinks;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.StringSerializer;

public class WriteStringSink extends PTransform<PCollection<String>, PDone> {

  String topic;

  public WriteStringSink(String topic) {

    this.topic = topic;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    /*return input.apply(
        "Write SenML strings to Kafka",
        KafkaIO.<Object, String>write()
            .withBootstrapServers("kafka-cluster-kafka-bootstrap:9092")
            .withTopic(this.topic)
            .withValueSerializer(StringSerializer.class)
            .values());*/
      input.apply(
          "No-Op Transform",
          ParDo.of(new DoFn<String, Void>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                  // Do nothing - this is a no-op
              }
          }));
      return PDone.in(input.getPipeline());
  }
}
