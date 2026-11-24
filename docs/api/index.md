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

::: resourcera.models.AdmissionState

::: resourcera.models.ResourceLease

## Adaptive concurrency

::: resourcera.concurrency.ConcurrencyController

::: resourcera.concurrency.ConcurrencySlot

::: resourcera.models.ConcurrencyRecommendation

::: resourcera.models.ConcurrencyState

::: resourcera.models.ConcurrencyAdjustment

## Remote concurrency

::: resourcera.remote.RemoteConcurrencyController

::: resourcera.remote.RemoteConcurrencyState

::: resourcera.remote.RemoteAdjustment

::: resourcera.remote.RemoteConcurrencyRejectedError

## Provider contracts

::: resourcera.accelerators.DeviceReading

::: resourcera.accelerators.ProviderReport

::: resourcera.models.ProviderInfo

## Errors

::: resourcera.exceptions
