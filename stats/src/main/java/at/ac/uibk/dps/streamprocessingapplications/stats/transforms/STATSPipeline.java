package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import at.ac.uibk.dps.streamprocessingapplications.shared.sinks.StoreStringInDBSink;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

public class STATSPipeline<T> extends PTransform<PCollection<String>, PDone> {
  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<String, T> parser;
  private final DoFn<Iterable<T>, Double> averagingFunction;
  private final DoFn<Iterable<T>, Long> distinctCountFunction;
  private final int batchSize;
  private final DoFn<KV<String, T>, KV<String, T>> kalmanFilterFunction;
  private final DoFn<KV<String, T>, KV<String, List<Double>>> slidingLinearRegressionFunction;

  public STATSPipeline(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<String, T> parser,
      DoFn<Iterable<T>, Double> averagingFunction,
      DoFn<Iterable<T>, Long> distinctCountFunction,
      int batchSize,
      DoFn<KV<String, T>, KV<String, T>> kalmanFilterFunction,
      DoFn<KV<String, T>, KV<String, List<Double>>> slidingLinearRegressionFunction) {
    this.typeDescriptor = typeDescriptor;
    this.parser = parser;
    this.averagingFunction = averagingFunction;
    this.distinctCountFunction = distinctCountFunction;
    this.batchSize = batchSize;
    this.kalmanFilterFunction = kalmanFilterFunction;
    this.slidingLinearRegressionFunction = slidingLinearRegressionFunction;
  }

  @Override
  public PDone expand(PCollection<String> input) {

    PCollection<T> parsedObjects =
        input.apply("Parse", MapElements.into(this.typeDescriptor).via(this.parser));

    PDone average =
        parsedObjects
            .apply("Average", new Average<>(this.averagingFunction, batchSize))
            .apply("Visualise", new Visualise<>(new AveragePlot(), 10))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Objects::toString))
            .apply(new StoreStringInDBSink("plots"));

    PDone kalmanAndPredict =
        parsedObjects
            .apply(
                "AddKey",
                ParDo.of(
                    new DoFn<T, KV<String, T>>() {
                      private int count = 0;

                      @ProcessElement
                      public void processElement(
                          @Element TaxiRide element, OutputReceiver<KV<String, TaxiRide>> out) {
                        int key = count % 2;
                        out.output(KV.of(String.valueOf(key), element));
                        count++;
                      }
                    }))
            .apply("KalmanFilter", ParDo.of(this.kalmanFilterFunction))
            .apply("SlidingLinearReg", ParDo.of(this.slidingLinearRegressionFunction))
            .apply(Values.create())
            .apply("Visualise", new Visualise<>(new KalmanRegressionPlot(), 10))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Objects::toString))
            .apply(new StoreStringInDBSink("plots"));

    PDone distinctCount =
        parsedObjects
            .apply(
                "Count Distinct", new DistinctCount<>(this.distinctCountFunction, this.batchSize))
            .apply("Visualise", new Visualise<>(new DistinctCountPlot(), 10))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Objects::toString))
            .apply(new StoreStringInDBSink("plots"));
    return distinctCount;
  }
}
