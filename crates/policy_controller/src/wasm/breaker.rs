//! The plugin's breaker: traps, exhausted budgets and invalid answers count
//! as failures within a window; a moving p50 past half its frame or a p99
//! past it (each call against its own frame) counts as slowness; either
//! opens the breaker for a cool-down, during which every stage of the plugin
//! takes its fallback without a call. One breaker per plugin, shared by its
//! instances; the latency sample rings are per instance (no shared write per
//! call).

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::config::BreakerSpec;

#[derive(Debug)]
pub(crate) struct Breaker {
    spec: BreakerSpec,
    failures: AtomicU32,
    window_start: AtomicU64,
    /// Microseconds on the runtime's clock until which the breaker is open;
    /// `0` when closed.
    open_until: AtomicU64,
    opens: AtomicU64,
}

impl Breaker {
    pub(crate) fn new(spec: BreakerSpec) -> Self {
        Self {
            spec,
            failures: AtomicU32::new(0),
            window_start: AtomicU64::new(0),
            open_until: AtomicU64::new(0),
            opens: AtomicU64::new(0),
        }
    }

    /// Whether the plugin is bypassed at `now`. A cool-down that has passed
    /// closes the breaker on this read: the calls resume, and the failure
    /// window counts afresh from the next failure.
    pub(crate) fn is_open(&self, now_micros: u64) -> bool {
        let until = self.open_until.load(Ordering::Relaxed);
        if until == 0 {
            return false;
        }
        if now_micros < until {
            return true;
        }
        let _ = self
            .open_until
            .compare_exchange(until, 0, Ordering::Relaxed, Ordering::Relaxed);
        false
    }

    /// One failure at `now`: opens the breaker at the window's threshold.
    pub(crate) fn failure(&self, now_micros: u64) {
        let start = self.window_start.load(Ordering::Relaxed);
        if now_micros.saturating_sub(start) > self.spec.window_micros {
            // A new window: the first failure in it. Two threads that race
            // here both count their failure in the new window.
            self.window_start.store(now_micros, Ordering::Relaxed);
            self.failures.store(0, Ordering::Relaxed);
        }
        if self.failures.fetch_add(1, Ordering::Relaxed) + 1 >= self.spec.failures {
            self.open(now_micros);
        }
    }

    /// The latency rule tripped: open at once.
    pub(crate) fn slow(&self, now_micros: u64) {
        self.open(now_micros);
    }

    fn open(&self, now_micros: u64) {
        self.open_until.store(
            now_micros.saturating_add(self.spec.cooldown_micros).max(1),
            Ordering::Relaxed,
        );
        self.failures.store(0, Ordering::Relaxed);
        self.opens.fetch_add(1, Ordering::Relaxed);
    }

    /// How many times the breaker opened.
    pub(crate) fn opens(&self) -> u64 {
        self.opens.load(Ordering::Relaxed)
    }
}

/// The last [`Latency::SAMPLES`] call durations of one instance, read every
/// [`Latency::EVERY`] calls, each sample the call's time as a share of its
/// own frame (in thousandths): a p50 past half the frame or a p99 past it is
/// slow. The p99 of a window is its second-slowest sample, so one outlier
/// does not open the breaker, and a window that read slow is forgotten, so
/// it does not open it twice.
#[derive(Debug)]
pub(crate) struct Latency {
    ring: [u32; Self::SAMPLES],
    next: usize,
    filled: usize,
}

impl Latency {
    pub(crate) const SAMPLES: usize = 128;
    pub(crate) const EVERY: usize = 64;
    /// The frame, in the ring's unit.
    const FRAME: u32 = 1_000;

    pub(crate) const fn new() -> Self {
        Self {
            ring: [0; Self::SAMPLES],
            next: 0,
            filled: 0,
        }
    }

    /// Records one call of `micros` under a frame of `frame_micros` (a call
    /// without a frame is not a sample); `true` when this call completed a
    /// window and the window reads slow, which forgets the window.
    pub(crate) fn record(&mut self, micros: u32, frame_micros: u32) -> bool {
        if frame_micros == 0 {
            return false;
        }
        let share = u64::from(micros) * u64::from(Self::FRAME) / u64::from(frame_micros);
        self.ring[self.next] = u32::try_from(share).unwrap_or(u32::MAX);
        self.next = (self.next + 1) % Self::SAMPLES;
        self.filled = (self.filled + 1).min(Self::SAMPLES);
        if !self.next.is_multiple_of(Self::EVERY) || self.filled < 2 {
            return false;
        }
        let mut sorted = [0u32; Self::SAMPLES];
        sorted[..self.filled].copy_from_slice(&self.ring[..self.filled]);
        let window = &mut sorted[..self.filled];
        window.sort_unstable();
        // Nearest rank for the median: the ceil(n / 2)-th smallest. The tail
        // is the window's second-slowest call, so one outlier is not it.
        let p50 = window[(self.filled.div_ceil(2)).saturating_sub(1)];
        let p99 = window[self.filled - 2];
        let slow = p50 > Self::FRAME / 2 || p99 > Self::FRAME;
        if slow {
            self.next = 0;
            self.filled = 0;
        }
        slow
    }
}

#[cfg(test)]
mod tests {
    use super::{Breaker, Latency};
    use crate::wasm::config::BreakerSpec;

    fn spec() -> BreakerSpec {
        BreakerSpec {
            failures: 3,
            window_micros: 1_000,
            cooldown_micros: 5_000,
        }
    }

    #[test]
    fn the_threshold_of_failures_within_a_window_opens_for_the_cooldown() {
        let b = Breaker::new(spec());
        b.failure(10);
        b.failure(20);
        assert!(!b.is_open(30));
        b.failure(30);
        assert!(b.is_open(31));
        assert!(b.is_open(5_029));
        assert!(!b.is_open(5_030), "the cool-down passed: closed again");
        assert_eq!(b.opens(), 1);
    }

    #[test]
    fn failures_in_separate_windows_do_not_add_up() {
        let b = Breaker::new(spec());
        b.failure(0);
        b.failure(500);
        b.failure(2_000);
        assert!(!b.is_open(2_001), "the third failure opened a new window");
        b.failure(2_100);
        b.failure(2_200);
        assert!(b.is_open(2_201));
    }

    #[test]
    fn slowness_opens_at_once_and_a_failure_after_the_cooldown_reopens() {
        let b = Breaker::new(spec());
        b.slow(100);
        assert!(b.is_open(101));
        assert!(!b.is_open(6_000));
        b.failure(6_001);
        b.failure(6_002);
        b.failure(6_003);
        assert!(b.is_open(6_004));
        assert_eq!(b.opens(), 2);
    }

    #[test]
    fn the_latency_window_reads_slow_on_p50_or_p99_past_the_frame() {
        let mut fast = Latency::new();
        let mut tripped = false;
        for _ in 0..Latency::SAMPLES {
            tripped |= fast.record(10, 100);
        }
        assert!(!tripped, "10 us calls under a 100 us frame are not slow");

        let mut half_slow = Latency::new();
        let mut tripped = false;
        for i in 0..Latency::SAMPLES {
            // One more than half the calls past half the frame: the median
            // lands on them.
            let micros = if i % 2 == 0 || i == 1 { 60 } else { 10 };
            tripped |= half_slow.record(micros, 100);
        }
        assert!(tripped, "a p50 past half the frame is slow");

        let mut one_outlier = Latency::new();
        let mut tripped = false;
        for i in 0..Latency::SAMPLES {
            let micros = if i == 5 { 500 } else { 10 };
            tripped |= one_outlier.record(micros, 100);
        }
        assert!(
            !tripped,
            "one call past the frame is not the p99 of the ring"
        );

        let mut tail = Latency::new();
        let mut tripped = false;
        for i in 0..Latency::SAMPLES {
            let micros = if i == 5 || i == 9 { 500 } else { 10 };
            tripped |= tail.record(micros, 100);
        }
        assert!(tripped, "two calls past the frame are the p99");
        let mut tripped_again = false;
        for _ in 0..Latency::SAMPLES {
            tripped_again |= tail.record(10, 100);
        }
        assert!(!tripped_again, "the window that read slow is forgotten");

        let mut mixed = Latency::new();
        let mut tripped = false;
        for i in 0..Latency::SAMPLES {
            // Sixty microseconds is slow under a 100 us frame and nothing
            // under a 1,000 us one: each call against its own frame.
            let frame = if i % 2 == 0 { 1_000 } else { 100 };
            tripped |= mixed.record(if frame == 1_000 { 60 } else { 10 }, frame);
        }
        assert!(!tripped, "each sample is judged against its own frame");

        let mut unbudgeted = Latency::new();
        assert!(!(0..Latency::SAMPLES).any(|_| unbudgeted.record(1_000, 0)));
    }
}
