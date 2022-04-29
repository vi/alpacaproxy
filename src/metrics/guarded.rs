// imported and adapted from https://docs.rs/prometheus/0.13.0/src/prometheus/gauge.rs.html#17-19
// to use prometheus 0.13 instead of 0.12.

use prometheus::core::{Atomic, AtomicF64, AtomicI64, GenericGauge, Number};

/// An RAII-style guard for an [`AtomicI64`] gauge.
///
/// Created by the methods on the [`GuardedGauge`] extension trait.
pub type IntGaugeGuard = GenericGaugeGuard<AtomicI64>;

/// An RAII-style guard for an [`AtomicF64`] gauge.
///
/// Created by the methods on the [`GuardedGauge`] extension trait.
pub type GaugeGuard = GenericGaugeGuard<AtomicF64>;

/// An RAII-style guard for situations where we want to increment a gauge and then ensure that there
/// is always a corresponding decrement.
///
/// Created by the methods on the [`GuardedGauge`] extension trait.
pub struct GenericGaugeGuard<P: Atomic + 'static> {
    value: P::T,
    gauge: &'static GenericGauge<P>,
}

/// When a gauge guard is dropped, it will perform the corresponding decrement.
impl<P: Atomic + 'static> Drop for GenericGaugeGuard<P> {
    fn drop(&mut self) {
        self.gauge.sub(self.value);
    }
}

/// An extension trait for [`GenericGauge`] to provide methods for temporarily modifying a gauge.
pub trait GuardedGauge<P: Atomic + 'static> {
    /// Increase the gauge by 1 while the guard exists.
    #[must_use]
    fn guarded_inc(&'static self) -> GenericGaugeGuard<P>;

    /// Increase the gauge by the given increment while the guard exists.
    #[must_use]
    fn guarded_add(&'static self, v: P::T) -> GenericGaugeGuard<P>;
}

impl<P: Atomic + 'static> GuardedGauge<P> for GenericGauge<P> {
    fn guarded_inc(&'static self) -> GenericGaugeGuard<P> {
        self.inc();
        GenericGaugeGuard {
            value: <P::T as Number>::from_i64(1),
            gauge: self,
        }
    }

    fn guarded_add(&'static self, v: P::T) -> GenericGaugeGuard<P> {
        self.add(v);
        GenericGaugeGuard {
            value: v,
            gauge: self,
        }
    }
}
