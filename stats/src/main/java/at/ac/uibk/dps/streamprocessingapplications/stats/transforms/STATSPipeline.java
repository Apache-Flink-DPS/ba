package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

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
  private final DoFn<T, T> kalmanFilterFunction;
  private final DoFn<T, List<Double>> slidingLinearRegressionFunction;

  public STATSPipeline(
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<String, T> parser,
      DoFn<Iterable<T>, Double> averagingFunction,
      DoFn<Iterable<T>, Long> distinctCountFunction,
      int batchSize,
      DoFn<T, T> kalmanFilterFunction,
      DoFn<T, List<Double>> slidingLinearRegressionFunction) {
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
            .apply("KalmanFilter", ParDo.of(this.kalmanFilterFunction))
            .apply("SlidingLinearReg", ParDo.of(this.slidingLinearRegressionFunction))
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
