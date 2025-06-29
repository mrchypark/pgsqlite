use crate::PgSqliteError;

/// Translates PostgreSQL JSON/JSONB types to SQLite-compatible types
pub struct JsonTranslator;

impl JsonTranslator {
    /// Translate SQL statement, converting JSON/JSONB types to TEXT
    pub fn translate_statement(sql: &str) -> Result<String, PgSqliteError> {
        // Quick check to avoid regex if not needed
        let lower_sql = sql.to_lowercase();
        if !lower_sql.contains("json") && !lower_sql.contains("jsonb") {
            return Ok(sql.to_string());
        }

        // For now, use simple string replacement for JSON/JSONB types
        // This is more reliable than trying to parse and modify the AST
        let mut result = sql.to_string();
        
        // Replace JSONB type (case-insensitive)
        result = Self::replace_type(&result, "JSONB", "TEXT");
        
        // Replace JSON type (case-insensitive)  
        result = Self::replace_type(&result, "JSON", "TEXT");
        
        Ok(result)
    }
    
    /// Replace a type name in SQL, preserving case and word boundaries
    fn replace_type(sql: &str, from_type: &str, to_type: &str) -> String {
        let regex_pattern = format!(r"\b{}\b", regex::escape(from_type));
        let re = regex::RegexBuilder::new(&regex_pattern)
            .case_insensitive(true)
            .build()
            .unwrap();
        re.replace_all(sql, to_type).to_string()
    }
    
    /// Check if a query is trying to use JSON/JSONB functions
    pub fn contains_json_operations(sql: &str) -> bool {
        let lower_sql = sql.to_lowercase();
        
        // PostgreSQL JSON operators and functions
        lower_sql.contains("->") ||
        lower_sql.contains("->>") ||
        lower_sql.contains("#>") ||
        lower_sql.contains("#>>") ||
        lower_sql.contains("@>") ||
        lower_sql.contains("<@") ||
        lower_sql.contains("?") ||
        lower_sql.contains("?|") ||
        lower_sql.contains("?&") ||
        lower_sql.contains("jsonb_") ||
        lower_sql.contains("json_") ||
        lower_sql.contains("to_json") ||
        lower_sql.contains("to_jsonb") ||
        lower_sql.contains("array_to_json") ||
        lower_sql.contains("row_to_json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_table_json_translation() {
        let sql = "CREATE TABLE test (id INTEGER, data JSON, metadata JSONB)";
        let translated = JsonTranslator::translate_statement(sql).unwrap();
        
        // Should convert JSON/JSONB to TEXT
        assert!(translated.contains("TEXT"));
        assert!(!translated.to_uppercase().contains("JSONB"));
        assert!(!translated.to_uppercase().contains(" JSON"));
    }
    
    #[test]
    fn test_alter_table_json_translation() {
        let sql = "ALTER TABLE test ADD COLUMN config JSONB";
        let translated = JsonTranslator::translate_statement(sql).unwrap();
        
        assert!(translated.contains("TEXT"));
        assert!(!translated.to_uppercase().contains("JSONB"));
    }
    
    #[test]
    fn test_json_operation_detection() {
        assert!(JsonTranslator::contains_json_operations("SELECT data->>'name' FROM users"));
        assert!(JsonTranslator::contains_json_operations("SELECT * WHERE config @> '{\"active\": true}'"));
        assert!(JsonTranslator::contains_json_operations("SELECT jsonb_array_length(items) FROM orders"));
        assert!(!JsonTranslator::contains_json_operations("SELECT * FROM users"));
    }
}