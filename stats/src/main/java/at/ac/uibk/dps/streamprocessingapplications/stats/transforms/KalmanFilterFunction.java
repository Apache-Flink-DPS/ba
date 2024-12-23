package at.ac.uibk.dps.streamprocessingapplications.stats.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class KalmanFilterFunction<T> extends DoFn<T, T> {

  // INFO: Values were ported from:
  // https://github.com/dream-lab/riot-bench/blob/c86414f7f926ed5ae0fab756bb3d82fbfb6e5bf7/modules/tasks/src/main/resources/tasks_TAXI.properties
  private static final double q_processNoise = 0.125;
  private static final double r_sensorNoise = 0.32;

  private SerializableFunction<T, Double> getter;
  private SerializableBiFunction<T, Double, T> setter;

  private int counter;

  public KalmanFilterFunction(
      SerializableFunction<T, Double> getter, SerializableBiFunction<T, Double, T> setter) {
    this.getter = getter;
    this.setter = setter;
    this.counter = 0;
  }

  @ProcessElement
  public void processElement(@Element T element, OutputReceiver<T> out) {
    if (counter == 5) {
      final double z_measuredValue = this.getter.apply(element);

      // NOTE: conditional override of the values due to `NullPointerException`
      double x0_previousEstimation = 0.0;
      try {
        x0_previousEstimation = 0.0;
      } catch (NullPointerException ignore) {
      }

      double p0_priorErrorCovariance = 0.0;
      try {
        p0_priorErrorCovariance = 0.0;
      } catch (NullPointerException ignore) {
      }

      // INFO: The following code was copied from the original `riot-bench` implementation.
      double p1_currentErrorCovariance = p0_priorErrorCovariance + q_processNoise;

      double k_kalmanGain = p1_currentErrorCovariance / (p1_currentErrorCovariance + r_sensorNoise);
      double x1_currentEstimation =
          x0_previousEstimation + k_kalmanGain * (z_measuredValue - x0_previousEstimation);
      p1_currentErrorCovariance = (1 - k_kalmanGain) * p1_currentErrorCovariance;

      out.output(this.setter.apply(element, x1_currentEstimation));
      counter = 0;
    }
    counter++;
  }
}
