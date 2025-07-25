use crate::record::TimeUnit;

/// Split a timestamp value into seconds and nanoseconds
pub(crate) fn split_second_ns(v: i64, unit: TimeUnit) -> (i64, u32) {
    let base = TimeUnit::Second.factor() / unit.factor();
    let sec = v.div_euclid(base);
    let nsec = v.rem_euclid(base) * unit.factor();
    (sec, nsec as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_ns() {
        let (sec, nsec) = split_second_ns(1716393600, TimeUnit::Second);
        assert_eq!(sec, 1716393600);
        assert_eq!(nsec, 0);

        let (sec, mills) = split_second_ns(1716393600, TimeUnit::Millisecond);
        assert_eq!(sec, 1716393);
        assert_eq!(mills, 600_000_000);

        let (sec, micros) = split_second_ns(1716393600, TimeUnit::Microsecond);
        assert_eq!(sec, 1716);
        assert_eq!(micros, 393_600_000);

        let (sec, nanos) = split_second_ns(1716393600, TimeUnit::Nanosecond);
        assert_eq!(sec, 1);
        assert_eq!(nanos, 716_393_600);
    }
}
