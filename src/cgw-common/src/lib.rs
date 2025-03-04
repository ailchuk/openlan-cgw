pub mod cgw_errors;
pub mod cgw_app_args;
pub mod cgw_tls;

use std::str::FromStr;

#[macro_use]
extern crate log;

#[derive(Copy, Clone)]
pub enum AppCoreLogLevel {
    /// Print debug-level messages and above
    Debug,
    /// Print info-level messages and above
    Info,
}

impl FromStr for AppCoreLogLevel {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "debug" => Ok(AppCoreLogLevel::Debug),
            "info" => Ok(AppCoreLogLevel::Info),
            _ => Err(()),
        }
    }
}