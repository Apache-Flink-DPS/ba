package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

public class DistinctCount<T> extends PTransform<PCollection<T>, PCollection<Long>> {
  private DoFn<Iterable<T>, Long> distinctCountFunction;
  private int batchSize;

  public DistinctCount(DoFn<Iterable<T>, Long> distinctCountFunction, int batchSize) {
    this.distinctCountFunction = distinctCountFunction;
    this.batchSize = batchSize;
  }

  @Override
  public PCollection<Long> expand(PCollection<T> input) {
    /* INFO:
     * `GroupIntoBatches` only supports grouping for key-value pairs.
     * Therefore, a pseudo mapping to the same key is performed.
     */
    return input
        .apply(
            "BatchVisAvg",
            ParDo.of(
                new DoFn<T, Iterable<T>>() {
                  private List<T> buffer = new ArrayList<>();
                  private int currentSize = 0;
                  private final int batchSize = DistinctCount.this.batchSize;

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
        .apply(ParDo.of(this.distinctCountFunction));
  }
}
