use crate::topic;

#[derive(Debug, PartialEq)]
pub enum Command {
    Sub(String),
    Unsub(String),
    Pub {
        topic: String,
        payload: String,
        retain: bool,
    },
    Stats,
    Ping,
    Quit,
}

pub fn parse(line: &str) -> Result<Command, String> {
    let mut parts = line.splitn(3, ' ');
    let verb = parts.next().unwrap_or("");
    match verb {
        "PUB" | "PUBR" => {
            let topic = parts
                .next()
                .filter(|t| !t.is_empty())
                .ok_or_else(|| "missing topic".to_owned())?;
            if !topic::valid_topic(topic) {
                return Err(format!("invalid topic '{topic}'"));
            }
            Ok(Command::Pub {
                topic: topic.to_owned(),
                payload: parts.next().unwrap_or("").to_owned(),
                retain: verb == "PUBR",
            })
        }
        "SUB" | "UNSUB" => {
            let pattern = parts
                .next()
                .filter(|p| !p.is_empty())
                .ok_or_else(|| "missing pattern".to_owned())?;
            if parts.next().is_some() {
                return Err("pattern must not contain spaces".to_owned());
            }
            if !topic::valid_pattern(pattern) {
                return Err(format!("invalid pattern '{pattern}'"));
            }
            let pattern = pattern.to_owned();
            Ok(if verb == "SUB" {
                Command::Sub(pattern)
            } else {
                Command::Unsub(pattern)
            })
        }
        "STATS" => Ok(Command::Stats),
        "PING" => Ok(Command::Ping),
        "QUIT" => Ok(Command::Quit),
        other => Err(format!("unknown command '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pub_with_spaces_in_payload() {
        assert_eq!(
            parse("PUB a/b hello world again"),
            Ok(Command::Pub {
                topic: "a/b".into(),
                payload: "hello world again".into(),
                retain: false,
            })
        );
    }

    #[test]
    fn parses_retained_pub_and_empty_payload() {
        assert_eq!(
            parse("PUBR cfg/x 5"),
            Ok(Command::Pub {
                topic: "cfg/x".into(),
                payload: "5".into(),
                retain: true,
            })
        );
        assert_eq!(
            parse("PUBR cfg/x"),
            Ok(Command::Pub {
                topic: "cfg/x".into(),
                payload: "".into(),
                retain: true,
            })
        );
    }

    #[test]
    fn parses_sub_and_rejects_garbage() {
        assert_eq!(parse("SUB a/+/c"), Ok(Command::Sub("a/+/c".into())));
        assert!(parse("SUB").is_err());
        assert!(parse("SUB a b").is_err());
        assert!(parse("SUB a/#/b").is_err());
        assert!(parse("PUB a/+ x").is_err());
        assert!(parse("NONSENSE").is_err());
    }

    #[test]
    fn parses_bare_commands() {
        assert_eq!(parse("PING"), Ok(Command::Ping));
        assert_eq!(parse("STATS"), Ok(Command::Stats));
        assert_eq!(parse("QUIT"), Ok(Command::Quit));
    }
}
