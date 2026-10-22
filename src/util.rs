// Small helpers shared by every mode.

use std::sync::{Mutex, MutexGuard};

// Lock a std Mutex, recovering it if a task panicked while holding it. The state behind these
// locks (pools, pins, cooldowns, caches) stays usable after a panic elsewhere, whereas
// `.lock().unwrap()` would turn one panic into a panic on every later access — taking the whole
// gateway down instead of one session.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| {
        // say so once (the flag is cleared): a panic happened while this state was being changed
        eprintln!("WARN recovered a lock poisoned by a panicked task; its state may be stale");
        m.clear_poison();
        poisoned.into_inner()
    })
}

// "a, b,,c" -> ["a", "b", "c"]: the comma-separated lists of every env setting.
pub(crate) fn split_csv(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_csv_splits_and_trims() {
        assert_eq!(
            split_csv("1.2.3.4, 5.6.7.8 ,,9.9.9.9"),
            vec!["1.2.3.4", "5.6.7.8", "9.9.9.9"]
        );
        assert!(split_csv("").is_empty());
        assert_eq!(
            split_csv("/hl/node1,/hl/node2"),
            vec!["/hl/node1", "/hl/node2"]
        );
    }

    #[test]
    fn lock_recovers_a_poisoned_mutex() {
        let m = std::sync::Arc::new(Mutex::new(vec![1]));
        let m2 = m.clone();
        let _ = std::thread::spawn(move || {
            let _g = m2.lock().unwrap();
            panic!("task panics while holding the lock");
        })
        .join();
        assert!(m.is_poisoned());
        lock(&m).push(2);
        assert!(
            !m.is_poisoned(),
            "recovered once, not on every later access"
        );
        assert_eq!(*lock(&m), [1, 2]);
    }
}
