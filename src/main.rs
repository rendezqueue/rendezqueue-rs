mod serve_options;

use std::{env, fs, process::ExitCode};

fn main() -> ExitCode {
    let paths: Vec<_> = env::args().skip(1).collect();
    if paths.first().is_some_and(|s| s == "serve") {
        let options = match serve_options::parse(&paths[1..]) {
            Ok(options) => options,
            Err(error) => {
                eprintln!("{error}\nusage: rendezqueue serve [bind-address] [--ttl SECONDS]");
                return ExitCode::FAILURE;
            }
        };
        let result = rendezqueue::http::HttpServer::bind_with_ttl(&options.bind, options.ttl)
            .and_then(|mut server| {
                println!(
                    "Listening on http://{}; POST /plyswap (JSON/query room); cache TTL {}s",
                    server.local_addr(),
                    options.ttl.as_secs()
                );
                server.run()
            });
        if let Err(e) = result {
            eprintln!("HTTP server failed: {e}");
            return ExitCode::FAILURE;
        }
        return ExitCode::SUCCESS;
    }
    if paths.is_empty() {
        eprintln!(
            "usage: rendezqueue serve [bind-address] [--ttl SECONDS] | <scenario.sxpb> [...]"
        );
        return ExitCode::FAILURE;
    }
    for path in paths {
        let result = fs::read_to_string(&path)
            .map_err(|e| e.to_string())
            .and_then(|s| rendezqueue::scenario::run(&s));
        match result {
            Ok(n) => println!("PASS {path}: {n} steps"),
            Err(e) => {
                eprintln!("{path}: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
