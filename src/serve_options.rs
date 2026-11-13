use std::time::Duration;

pub(crate) struct ServeOptions {
    pub(crate) bind: String,
    pub(crate) ttl: Duration,
}

pub(crate) fn parse(args: &[String]) -> Result<ServeOptions, &'static str> {
    let mut bind = None;
    let mut ttl = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--ttl" {
            if ttl.is_some() {
                return Err("--ttl may appear only once");
            }
            let seconds = args
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|seconds| *seconds != 0)
                .ok_or("--ttl requires positive integer seconds")?;
            ttl = Some(Duration::from_secs(seconds));
        } else if arg.starts_with('-') || bind.is_some() {
            return Err("expected one bind address and optional --ttl SECONDS");
        } else {
            bind = Some(arg.clone());
        }
    }
    Ok(ServeOptions {
        bind: bind.unwrap_or_else(|| "127.0.0.1:15050".into()),
        ttl: ttl.unwrap_or(rendezqueue::http::DEFAULT_CACHE_TTL),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(args: &[&str]) -> Result<ServeOptions, &'static str> {
        parse(&args.iter().map(|s| (*s).into()).collect::<Vec<_>>())
    }

    #[test]
    fn defaults_and_configured_ttl() {
        let defaults = options(&[]).unwrap();
        assert_eq!(defaults.bind, "127.0.0.1:15050");
        assert_eq!(defaults.ttl, Duration::from_secs(20));
        for args in [["127.0.0.1:0", "--ttl", "7"], ["--ttl", "7", "127.0.0.1:0"]] {
            let configured = options(&args).unwrap();
            assert_eq!(configured.bind, "127.0.0.1:0");
            assert_eq!(configured.ttl, Duration::from_secs(7));
        }
    }

    #[test]
    fn zero_missing_duplicate_unknown_and_overflow_are_errors() {
        for args in [
            vec!["--ttl"],
            vec!["--ttl", "0"],
            vec!["--ttl", "-1"],
            vec!["--ttl", "0.5"],
            vec!["--ttl", "18446744073709551616"],
            vec!["--ttl", "1", "--ttl", "2"],
            vec!["--wat"],
            vec!["--ttl-seconds", "20"],
            vec!["127.0.0.1:0", "127.0.0.1:1"],
        ] {
            assert!(options(&args).is_err(), "{args:?}");
        }
    }
}
