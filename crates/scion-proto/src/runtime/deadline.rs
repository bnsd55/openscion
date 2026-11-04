//! A caller's deadline, as gRPC sends it in `grpc-timeout`, and the part of
//! it that each process keeps for its answer.
//!
//! A process that gets a call with a `grpc-timeout` must answer before that
//! runs out. So it ends its own waits for the call a margin earlier, at
//! [`Until`], and gives each call that it makes for this one what is left,
//! as its `grpc-timeout`. Each process down the line then ends its waits
//! before the one above it, and the status of the deepest one reaches the
//! caller in time. The margin is a tenth of the timeout, but at least
//! `MIN_MARGIN` and never more than half of the timeout, at most
//! `MAX_MARGIN`: room for the answer to travel back, and a short deadline
//! still leaves most of its time to the deepest process.

use std::time::{Duration, Instant};

use tonic::Request;
use tonic::metadata::MetadataMap;

/// The most that a process keeps of a caller's timeout for its answer.
pub(crate) const MAX_MARGIN: Duration = Duration::from_secs(1);

/// The least that a process keeps of a caller's timeout for its answer:
/// the margin is the reply's trip back, and that trip does not shrink
/// with the deadline.
const MIN_MARGIN: Duration = Duration::from_millis(50);

/// The instant by which a process ends its waits for a call. With no
/// deadline from the caller, only the fixed limits of each wait apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Until(Option<Instant>);

impl Until {
    /// No deadline.
    pub const NONE: Self = Self(None);

    /// For `request`, received now: its `grpc-timeout` less the margin.
    /// [`Until::NONE`] if it has none, or one that is not valid.
    #[must_use]
    pub fn of<T>(request: &Request<T>) -> Self {
        grpc_timeout(request.metadata()).map_or(Self::NONE, Self::after)
    }

    /// For a call whose caller gives up `timeout` from now.
    #[must_use]
    pub fn after(timeout: Duration) -> Self {
        Self::after_at(timeout, Instant::now())
    }

    /// For a call whose caller gives up `timeout` from `now`: the same
    /// margin as [`Until::after`], but the instant is measured from the
    /// `now` the caller holds, not from a second read of the clock. A
    /// hop that builds its header and its deadline from one `now` keeps
    /// its whole margin, whatever a pause between the two work items
    /// takes - a pause after `now` moves the header and the deadline
    /// together.
    #[must_use]
    pub fn after_at(timeout: Duration, now: Instant) -> Self {
        let margin = (timeout / 10)
            .max(MIN_MARGIN)
            .min(MAX_MARGIN)
            .min(timeout / 2);
        Self(now.checked_add(timeout.saturating_sub(margin)))
    }

    /// The instant, if there is a deadline.
    #[must_use]
    pub fn at(self) -> Option<Instant> {
        self.0
    }

    /// How long a wait with the fixed limit `cap` may last: `cap`, or what is
    /// left until this if that is shorter. Zero once this has passed.
    #[must_use]
    pub fn wait(self, cap: Duration) -> Duration {
        self.0.map_or(cap, |at| {
            cap.min(at.saturating_duration_since(Instant::now()))
        })
    }

    /// Whether this has passed.
    #[must_use]
    pub fn passed(self) -> bool {
        self.0.is_some_and(|at| at <= Instant::now())
    }

    /// Whether this comes before `other`. No deadline comes after all.
    #[must_use]
    pub fn before(self, other: Self) -> bool {
        match (self.0, other.0) {
            (Some(at), Some(other)) => at < other,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// `message`, as a call to the next process, with what is left until
    /// this as its `grpc-timeout`.
    #[must_use]
    pub fn request<T>(self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(at) = self.0 {
            request.set_timeout(at.saturating_duration_since(Instant::now()));
        }
        request
    }
}

/// The `grpc-timeout` in `metadata`, as the gRPC spec writes it: 1 to 8
/// digits and a unit (`H`, `M`, `S`, `m`, `u` or `n`). `None` if there is
/// none, or it is not valid.
fn grpc_timeout(metadata: &MetadataMap) -> Option<Duration> {
    let value = metadata.get("grpc-timeout")?.to_str().ok()?;
    let unit = value.chars().last()?;
    let digits = &value[..value.len() - unit.len_utf8()];
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        'H' => Duration::from_secs(n * 3600),
        'M' => Duration::from_secs(n * 60),
        'S' => Duration::from_secs(n),
        'm' => Duration::from_millis(n),
        'u' => Duration::from_micros(n),
        'n' => Duration::from_nanos(n),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_timeout(value: &str) -> Request<()> {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("grpc-timeout", value.parse().unwrap());
        request
    }

    #[test]
    fn grpc_timeout_follows_the_spec() {
        let parse = |value| grpc_timeout(with_timeout(value).metadata());
        assert_eq!(parse("2H"), Some(Duration::from_secs(7200)));
        assert_eq!(parse("3M"), Some(Duration::from_secs(180)));
        assert_eq!(parse("30S"), Some(Duration::from_secs(30)));
        assert_eq!(parse("250m"), Some(Duration::from_millis(250)));
        assert_eq!(parse("7u"), Some(Duration::from_micros(7)));
        assert_eq!(parse("99999999n"), Some(Duration::from_nanos(99_999_999)));
        for bad in ["", "S", "123456789S", "10s", "1.5S", "-1S", "10"] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
        assert_eq!(grpc_timeout(Request::new(()).metadata()), None);
    }

    /// What one process sends, the next reads back, less its margin.
    #[test]
    fn a_deadline_passes_down_less_a_margin() {
        let sent = Until::after(Duration::from_secs(100)).request(());
        let first = Until::of(&sent).at().unwrap();
        let wait = first.saturating_duration_since(Instant::now());
        // 100 s, less 1 s here and at most 1 s there, and the time between.
        assert!(wait > Duration::from_secs(97) && wait <= Duration::from_secs(98));

        let short = Until::after(Duration::from_millis(500)).wait(Duration::MAX);
        assert!(short > Duration::from_millis(400) && short <= Duration::from_millis(450));
        assert_eq!(Until::of(&Request::new(())), Until::NONE);
    }

    #[test]
    fn a_wait_is_the_shorter_of_its_cap_and_the_deadline() {
        let cap = Duration::from_secs(30);
        assert_eq!(Until::NONE.wait(cap), cap);
        assert_eq!(Until::after(Duration::from_secs(3600)).wait(cap), cap);
        assert!(Until::after(Duration::from_secs(5)).wait(cap) < Duration::from_secs(5));
        assert_eq!(Until::after(Duration::ZERO).wait(cap), Duration::ZERO);
        assert!(Until::after(Duration::ZERO).passed());
        assert!(!Until::NONE.passed());
    }

    /// The margin is a tenth of the timeout, but at least the floor and
    /// never more than half of the timeout, at most the cap.
    #[test]
    fn the_margin_has_a_floor_and_two_caps() {
        // `Until` starts at `now`, so a read-back loses the few
        // microseconds between; one ms of slack absorbs the truncation.
        let kept = |timeout: Duration| {
            let at = Until::after(timeout).at().unwrap();
            at.saturating_duration_since(Instant::now()).as_millis()
        };
        let is = |got: u128, want: u128| got + 1 >= want && got <= want;
        // Under the floor: half of the timeout caps the floor.
        assert!(
            is(kept(Duration::from_millis(80)), 40),
            "{}",
            kept(Duration::from_millis(80))
        );
        // At the floor.
        assert!(
            is(kept(Duration::from_millis(200)), 150),
            "{}",
            kept(Duration::from_millis(200))
        );
        assert!(
            is(kept(Duration::from_millis(500)), 450),
            "{}",
            kept(Duration::from_millis(500))
        );
        // The tenth above the floor.
        assert!(
            is(kept(Duration::from_secs(1)), 900),
            "{}",
            kept(Duration::from_secs(1))
        );
        assert!(
            is(kept(Duration::from_secs(4)), 3600),
            "{}",
            kept(Duration::from_secs(4))
        );
        // The cap: a tenth over 1 s keeps 1 s.
        assert!(
            is(kept(Duration::from_secs(20)), 19_000),
            "{}",
            kept(Duration::from_secs(20))
        );
    }

    /// A 200 ms deadline through three processes: each keeps its margin,
    /// capped at half of what it got, so the deepest answers with 50 ms
    /// of the caller's 200 ms still ahead of it.
    ///
    /// Each hop reads the clock once: the header's value and the hop's
    /// deadline both come from that one `now` (`Until::after_at`), so a
    /// pause anywhere inside the hop moves the header and the deadline
    /// together and changes no margin. Only a pause between the hops
    /// moves the instants apart, and the margins are read as deltas
    /// between them, which such a pause cannot change either.
    #[test]
    fn a_200ms_deadline_reaches_the_deepest_of_three_with_time_to_spare() {
        let pass = |left: Option<Instant>| {
            let now = Instant::now();
            let mut request = Request::new(());
            let left = left.map(|at| {
                let left = at.saturating_duration_since(now);
                let value = format!("{}m", left.as_millis());
                request
                    .metadata_mut()
                    .insert("grpc-timeout", value.parse().unwrap());
                // The header parses back to what it was made from, and
                // the deadline is measured from the same `now`: a second
                // read of the clock is the one a pause can split from the
                // header's, which took the pause out of the margin.
                grpc_timeout(request.metadata()).expect("the header parses back")
            });
            left.map_or(Until::NONE, |left| Until::after_at(left, now)).at()
        };
        let first = Until::after(Duration::from_millis(200)).at();
        let second = pass(first);
        let third = pass(second);
        // What each hop kept: the daemon 50 ms, the cache 50 ms, the
        // deepest 50 ms (its half cap). The margins are read as deltas
        // between the instants, so a scheduler slice between the hops
        // moves them all and changes no margin.
        let between = |a: Option<Instant>, b: Option<Instant>| {
            a.unwrap().saturating_duration_since(b.unwrap()).as_millis()
        };
        let kept1 = between(first, second);
        let kept2 = between(second, third);
        assert!((40..=60).contains(&kept1), "{}", kept1);
        assert!((40..=60).contains(&kept2), "{}", kept2);
        // The deepest still has its margin ahead of it: the caller's
        // 200 ms is not spent by the two hops above it.
        assert!(third.unwrap() > Instant::now());
    }

    #[test]
    fn no_deadline_comes_after_all() {
        let (soon, later) = (
            Until::after(Duration::from_secs(1)),
            Until::after(Duration::from_secs(9)),
        );
        assert!(soon.before(later) && !later.before(soon));
        assert!(later.before(Until::NONE) && !Until::NONE.before(later));
        assert!(!Until::NONE.before(Until::NONE) && !soon.before(soon));
    }
}
