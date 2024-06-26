#[cfg(feature = "completion")]
#[path = "src/cli.rs"]
mod cli;

fn main() {
    // Do NOT rerun on src changes.
    println!("cargo::rerun-if-changed=build.rs");

    #[cfg(feature = "completion")]
    {
        use clap::ValueEnum;
        use clap_complete::{generate_to, shells::Shell};

        let out_dir = std::path::Path::new("completions");
        let pkg_name = std::env::var("CARGO_PKG_NAME").expect("have CARGO_PKG_NAME");
        let mut cmd = <cli::Cli as clap::CommandFactory>::command();
        for &shell in Shell::value_variants() {
            let out_dir = out_dir.join(shell.to_string());
            std::fs::create_dir_all(&out_dir).expect("create_dir_all");
            if let Err(err) = generate_to(shell, &mut cmd, &pkg_name, &out_dir) {
                panic!("failed to generate completion for {shell}: {err}");
            }
        }
    }

    println!("cargo::rustc-check-cfg=cfg(not_in_build_rs)");
    println!("cargo::rustc-cfg=not_in_build_rs");
}
