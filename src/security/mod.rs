// Module for security functionality
pub mod audit_logger;
pub mod sql_injection_detector;

pub use audit_logger::{
    AuditConfig, AuditError, AuditStats, SecurityAuditLogger, SecurityEvent, SecurityEventType,
    SecuritySeverity, events, global_audit_logger, log_security_event,
};

pub use sql_injection_detector::{SqlAnalysisResult, SqlInjectionDetector};
