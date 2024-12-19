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
            "BufferElements",
            ParDo.of(
                new DoFn<T, Iterable<T>>() {
                  private List<T> buffer = new ArrayList<>();
                  private int currentSize = 0;
                  private final int batchSize = 100; // You can set this dynamically if needed

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    buffer.add(c.element());
                    currentSize++;

                    // Once we reach the batch size, output the buffer
                    if (currentSize >= batchSize) {
                      c.output(buffer); // Pass the batch of elements as an Iterable
                      buffer.clear(); // Reset the buffer
                      currentSize = 0;
                    }
                  }
                }))
        .apply("CalculateAverage", ParDo.of(averagingFunction));
  }
}
