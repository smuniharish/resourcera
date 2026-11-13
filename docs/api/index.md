# Public API

The top-level `resourcera` package lazily exposes the manager, configuration,
models, provider contracts, and exceptions.

## Manager

::: resourcera.manager.ResourceManager

## Configuration

::: resourcera.config.ResourceConfig

## Requirements, snapshots, and decisions

::: resourcera.models.WorkloadRequirements

::: resourcera.models.ResourceSnapshot

::: resourcera.models.Metric

::: resourcera.models.CapacityResult

::: resourcera.models.AdmissionDecision

::: resourcera.models.ResourceLease

## Adaptive concurrency

::: resourcera.concurrency.ConcurrencyController

::: resourcera.concurrency.ConcurrencySlot

::: resourcera.models.ConcurrencyRecommendation

::: resourcera.models.ConcurrencyState

::: resourcera.models.ConcurrencyAdjustment

## Provider contracts

::: resourcera.accelerators.DeviceReading

::: resourcera.accelerators.ProviderReport

::: resourcera.models.ProviderInfo

## Errors

::: resourcera.exceptions
