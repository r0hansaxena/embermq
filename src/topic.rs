pub fn valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && topic
            .split('/')
            .all(|level| !level.is_empty() && level != "+" && level != "#")
}

pub fn valid_pattern(pattern: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }
    let levels: Vec<&str> = pattern.split('/').collect();
    levels.iter().enumerate().all(|(i, level)| match *level {
        "#" => i == levels.len() - 1,
        "" => false,
        _ => true,
    })
}

pub fn matches(pattern: &str, topic: &str) -> bool {
    let mut pattern_levels = pattern.split('/');
    let mut topic_levels = topic.split('/');
    loop {
        match (pattern_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(p), Some(t)) if p == t => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(matches("a/b/c", "a/b/c"));
        assert!(!matches("a/b/c", "a/b"));
        assert!(!matches("a/b", "a/b/c"));
        assert!(!matches("a/b/c", "a/b/x"));
    }

    #[test]
    fn single_level_wildcard() {
        assert!(matches("a/+/c", "a/b/c"));
        assert!(matches("+/+/+", "a/b/c"));
        assert!(!matches("a/+", "a/b/c"));
        assert!(!matches("a/+/c", "a/c"));
    }

    #[test]
    fn multi_level_wildcard() {
        assert!(matches("#", "a"));
        assert!(matches("#", "a/b/c"));
        assert!(matches("a/#", "a/b/c"));
        assert!(matches("a/#", "a"));
        assert!(!matches("a/#", "b/c"));
    }

    #[test]
    fn topic_validation() {
        assert!(valid_topic("vehicle/42/engine"));
        assert!(!valid_topic(""));
        assert!(!valid_topic("a//b"));
        assert!(!valid_topic("a/+/b"));
        assert!(!valid_topic("a/#"));
    }

    #[test]
    fn pattern_validation() {
        assert!(valid_pattern("a/+/c"));
        assert!(valid_pattern("a/#"));
        assert!(valid_pattern("#"));
        assert!(!valid_pattern("a/#/c"));
        assert!(!valid_pattern("a//b"));
        assert!(!valid_pattern(""));
    }
}
