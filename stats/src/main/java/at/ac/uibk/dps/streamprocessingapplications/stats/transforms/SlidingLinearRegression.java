package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.math3.stat.regression.SimpleRegression;

public class SlidingLinearRegression<T> extends DoFn<T, List<Double>> {
  private final int trainWindowSize;
  private final int predictionHorizonSize;

  private SerializableFunction<T, Double> getter;

  public SlidingLinearRegression(
      SerializableFunction<T, Double> getter, int trainWindowSize, int predictionHorizonSize) {
    this.getter = getter;
    this.trainWindowSize = trainWindowSize;
    this.predictionHorizonSize = predictionHorizonSize;
  }

  @ProcessElement
  public void processElement(@Element T element, OutputReceiver<List<Double>> out) {

    Double newValue = this.getter.apply(element);
    List<Double> pastElements = new ArrayList<>();

    // Update `pastElements`
    pastElements.add(newValue);
    if (pastElements.size() > this.trainWindowSize) {
      pastElements.remove(0);
    }

    // Populate regression
    SimpleRegression regression = new SimpleRegression();
    // NOTE: The regression needs pairs of x and y values.
    // The original implementation uses a global counter, but I think it can also be done with
    // relative counters.
    for (int i = 0; i < pastElements.size(); i++) {
      regression.addData(i, pastElements.get(i));
    }

    // Predict `predictionHorizonSize` many elements
    List<Double> predictions = new ArrayList<>();
    for (int i = 0; i < this.predictionHorizonSize; i++) {
      predictions.add(regression.predict(pastElements.size() + i));
    }

    out.output(predictions);
  }
}
