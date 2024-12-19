package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

public class Average<T, Double> extends PTransform<PCollection<T>, PCollection<Double>> {
  private DoFn<Iterable<T>, Double> averagingFunction;
  private int batchSize;
  private int currentSize;

  public Average(DoFn<Iterable<T>, Double> averagingFunction, int batchSize) {
    this.averagingFunction = averagingFunction;
    this.batchSize = batchSize;
    this.currentSize = 0;
  }

  @Override
  public PCollection<Double> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */

    return input
        .apply(
            "BufferElements",
            ParDo.of(
                new DoFn<T, List<T>>() {
                  private final List<T> buffer = new ArrayList<>();
                  private final Object lock = new Object();

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    synchronized (lock) {
                      buffer.add(c.element()); // Add element to the buffer
                      if (buffer.size() >= batchSize) {
                        c.output(new ArrayList<>(buffer)); // Output a batch as a new list
                        buffer.clear(); // Clear the buffer
                      }
                    }
                  }

                  @FinishBundle
                  public void finishBundle(FinishBundleContext c) {
                    synchronized (lock) {
                      if (!buffer.isEmpty()) {
                        c.output(new ArrayList<>(buffer), Instant.now(), GlobalWindow.INSTANCE);
                        buffer.clear();
                      }
                    }
                  }
                }))
        .apply("AverageElements", ParDo.of(averagingFunction));
  }
}
