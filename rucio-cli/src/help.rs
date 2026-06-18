//! Localization of clap's `--help` text.
//!
//! clap bakes command/argument help into the binary from doc comments at
//! compile time, so it can't go through `t!` directly. Instead we build the
//! `Command`, then walk it and override every `about`/`help` string at runtime
//! with a translation keyed by the command path:
//!
//!   help.<sub>.<sub>.about            (a command's description)
//!   help.<sub>.<sub>.arg.<id>         (an argument's description)
//!
//! A key that has no translation in the active locale falls back to clap's
//! original (English) text, so partial catalogues degrade gracefully.

use clap::Command;
use rust_i18n::t;

/// Look up `key` in the active locale, returning `None` when it is missing
/// (rust-i18n echoes the key back when there is no translation).
fn tr(key: &str) -> Option<String> {
    let value = t!(key);
    if value == key {
        None
    } else {
        Some(value.to_string())
    }
}

/// Recursively translate a command's description, its arguments' help, and all
/// of its subcommands. `base` is the key prefix for this command (the root is
/// called with `"help"`).
pub fn localize(mut cmd: Command, base: &str) -> Command {
    if let Some(about) = tr(&format!("{base}.about")) {
        cmd = cmd.about(about);
    }

    cmd = cmd.mut_args(|arg| {
        let key = format!("{base}.arg.{}", arg.get_id());
        match tr(&key) {
            Some(help) => arg.help(help),
            None => arg,
        }
    });

    // clap's built-in -h/--help, -V/--version flags and the auto-generated
    // `help` subcommand are added lazily during build, so they don't show up
    // here and keep clap's English text — as do the "Usage:"/"Options:"
    // section headings, which live in clap's render template.
    let names: Vec<String> = cmd
        .get_subcommands()
        .map(|s| s.get_name().to_string())
        .filter(|n| n != "help")
        .collect();
    for name in names {
        let child_base = format!("{base}.{name}");
        cmd = cmd.mut_subcommand(&name, move |sub| localize(sub, &child_base));
    }

    cmd
}
