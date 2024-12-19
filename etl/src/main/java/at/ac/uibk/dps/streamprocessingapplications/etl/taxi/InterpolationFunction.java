package at.ac.uibk.dps.streamprocessingapplications.etl.taxi;

import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class InterpolationFunction
    implements SerializableFunction<Iterable<TaxiRide>, Iterable<TaxiRide>> {

  public InterpolationFunction() {}

  @Override
  public Iterable<TaxiRide> apply(Iterable<TaxiRide> taxiRides) {
    double meanTripTime = calculateMean(taxiRides, TaxiRide::getTripTimeInSecs);
    double meanTripDistance = calculateMean(taxiRides, TaxiRide::getTripDistance);
    double meanFareAmount = calculateMean(taxiRides, TaxiRide::getFareAmount);
    double meanTollsAmount = calculateMean(taxiRides, TaxiRide::getTollsAmount);
    double meanTotalAmount = calculateMean(taxiRides, TaxiRide::getTotalAmount);

    return StreamSupport.stream(taxiRides.spliterator(), false)
        .map(
            taxiRide ->
                interpolate(
                    taxiRide,
                    meanTripTime,
                    meanTripDistance,
                    meanFareAmount,
                    meanTollsAmount,
                    meanTotalAmount))
        .collect(Collectors.toList());
  }

  private TaxiRide interpolate(
      TaxiRide taxiRide,
      double meanTripTime,
      double meanTripDistance,
      double meanFareAmount,
      double meanTollsAmount,
      double meanTotalAmount) {
    if (taxiRide.getTripTimeInSecs().isEmpty()) {
      taxiRide.setTripTimeInSecs(meanTripTime);
    }
    if (taxiRide.getTripDistance().isEmpty()) {
      taxiRide.setTripDistance(meanTripDistance);
    }
    if (taxiRide.getFareAmount().isEmpty()) {
      taxiRide.setFareAmount(meanFareAmount);
    }
    if (taxiRide.getTollsAmount().isEmpty()) {
      taxiRide.setTollsAmount(meanTollsAmount);
    }
    if (taxiRide.getTotalAmount().isEmpty()) {
      taxiRide.setTotalAmount(meanTotalAmount);
    }

    return taxiRide;
  }

  private double calculateMean(
      Iterable<TaxiRide> taxiRides, Function<TaxiRide, Optional<Double>> getter) {
    Collection<Double> validValues = new ArrayList<>();
    for (TaxiRide taxiRide : taxiRides) {
      getter.apply(taxiRide).ifPresent(validValues::add);
    }
    return validValues.stream().mapToDouble(d -> d).average().orElse(0.0);
  }
}
