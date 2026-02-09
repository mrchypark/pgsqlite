// Module for query execution
pub mod comment_stripper;
pub mod executor;
pub mod extended;
pub mod extended_fast_path;
mod extended_helpers;
pub mod fast_path;
pub mod join_type_inference;
pub mod lazy_processor;
pub mod parameter_parser;
pub mod pattern_optimizer;
pub mod query_handler;
pub mod query_processor;
pub mod query_type_detection;
pub mod set_handler;
pub mod simple_query_detector;
pub mod unified_processor;

pub use comment_stripper::strip_sql_comments;
pub use executor::QueryExecutor;
pub use extended::ExtendedQueryHandler;
pub use fast_path::{
    FastPathOperation, FastPathQuery, WhereClause, can_use_fast_path, can_use_fast_path_enhanced,
    clear_decimal_cache, execute_fast_path, execute_fast_path_enhanced,
    execute_fast_path_enhanced_with_params, query_fast_path, query_fast_path_enhanced,
    query_fast_path_enhanced_with_params,
};
pub use lazy_processor::LazyQueryProcessor;
pub use parameter_parser::ParameterParser;
pub use pattern_optimizer::{
    OptimizationHints, QueryComplexity, QueryPattern, QueryPatternOptimizer, ResultSize,
};
pub use query_handler::{QueryHandler, QueryHandlerImpl};
pub use query_processor::process_query;
pub use query_type_detection::{QueryType, QueryTypeDetector};
pub use set_handler::SetHandler;
