package io.github.federiconbaez.outbox.application;

import java.util.Map;

public record RelayReport(int released, int claimed, Map<ProcessOutcome, Integer> outcomes) {
  public RelayReport {
    outcomes = Map.copyOf(outcomes);
  }

  public int count(ProcessOutcome outcome) {
    return outcomes.getOrDefault(outcome, 0);
  }
}
