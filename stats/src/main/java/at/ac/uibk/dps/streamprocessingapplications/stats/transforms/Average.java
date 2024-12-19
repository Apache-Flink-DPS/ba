package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
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
    return input.apply(
        ParDo.of(
            new DoFn<T, Double>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                currentSize += 1;
                taxiRides.add(c.element());
                if (currentSize >= batchSize) {
                  java.lang.Double result =
                      (java.lang.Double)
                          StreamSupport.stream(taxiRides.spliterator(), false)
                              .mapToDouble(
                                  taxiRide -> ((TaxiRide) taxiRide).getTripDistance().orElse(0.0))
                              .average()
                              .orElse(java.lang.Double.NaN);
                  c.output((Double) result);
                }
                currentSize = 0;
              }
            }));
  }
}
