package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

public class Visualise<T> extends PTransform<PCollection<T>, PCollection<byte[]>> {

  private DoFn<Iterable<TimestampedValue<T>>, byte[]> plottingFunction;
  private int batchSize;

  public Visualise(DoFn<Iterable<TimestampedValue<T>>, byte[]> plottingFunction, int batchSize) {
    this.plottingFunction = plottingFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<byte[]> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        .apply(new AddTimestamp<>())
        .apply(
            "BatchVisAvg",
            ParDo.of(
                new DoFn<TimestampedValue<T>, Iterable<TimestampedValue<T>>>() {
                  private List<TimestampedValue<T>> buffer = new ArrayList<>();
                  private int currentSize = 0;
                  private final int batchSize = Visualise.this.batchSize;

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    buffer.add(c.element());
                    currentSize++;
                    if (currentSize >= batchSize) {
                      c.output(buffer);
                      buffer.clear();
                      currentSize = 0;
                    }
                  }
                }))
        .apply("VisualizeAvg", ParDo.of(plottingFunction));
  }
}
