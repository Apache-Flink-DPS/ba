package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class Average<T, Double> extends PTransform<PCollection<T>, PCollection<Double>> {
  private DoFn<Iterable<T>, Double> averagingFunction;
  private int batchSize;
  private int currentSize;
  private List<T> taxiRides;

  public Average(DoFn<Iterable<T>, Double> averagingFunction, int batchSize) {
    this.averagingFunction = averagingFunction;
    this.batchSize = batchSize;
    this.currentSize = 0;
    this.taxiRides = new ArrayList<>();
  }

  @Override
  public PCollection<Double> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        .apply(
            "BatchAvg",
            ParDo.of(
                new DoFn<T, Iterable<T>>() {
                  private List<T> buffer = new ArrayList<>();
                  private int currentSize = 0;
                  private final int batchSize = Average.this.batchSize;

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
        .apply("Average", ParDo.of(averagingFunction));
  }
}
