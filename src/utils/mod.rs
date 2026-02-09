pub mod oid_generator;

pub use oid_generator::{generate_oid, generate_oid_i32, generate_oid_string};

// Unit tests in this crate frequently mutate process-wide environment variables.
// Guard those mutations to avoid cross-test interference under parallel test runs.
#[cfg(test)]
pub mod test_env;
