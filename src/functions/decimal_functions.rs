use rusqlite::{Connection, Result, functions::{FunctionFlags, Context}};
use rust_decimal::Decimal;
use std::str::FromStr;

/// Register all decimal-related functions with the SQLite connection
pub fn register_decimal_functions(conn: &Connection) -> Result<()> {
    // Storage functions
    conn.create_scalar_function(
        "decimal_from_text",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_from_text,
    )?;
    
    conn.create_scalar_function(
        "decimal_to_text",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_to_text,
    )?;
    
    // Arithmetic functions
    conn.create_scalar_function(
        "decimal_add",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_add,
    )?;
    
    conn.create_scalar_function(
        "decimal_sub",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_sub,
    )?;
    
    conn.create_scalar_function(
        "decimal_mul",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_mul,
    )?;
    
    conn.create_scalar_function(
        "decimal_div",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_div,
    )?;
    
    // Comparison functions
    conn.create_scalar_function(
        "decimal_eq",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_eq,
    )?;
    
    conn.create_scalar_function(
        "decimal_lt",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_lt,
    )?;
    
    conn.create_scalar_function(
        "decimal_gt",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_gt,
    )?;
    
    // Note: Aggregate functions will be implemented using SQLite's built-in aggregates
    // with proper wrapping of decimal values. For now, we provide the scalar functions
    // that can be used with GROUP BY for basic aggregation.
    
    Ok(())
}

// Storage functions

fn decimal_from_text(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match ctx.get_raw(0) {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            match Decimal::from_str(text) {
                Ok(decimal) => {
                    // Store as binary representation
                    let bytes = decimal.serialize();
                    Ok(Some(bytes.to_vec()))
                }
                Err(e) => Err(rusqlite::Error::UserFunctionError(Box::new(e)))
            }
        }
        rusqlite::types::ValueRef::Integer(i) => {
            let decimal = Decimal::new(i, 0);
            Ok(Some(decimal.serialize().to_vec()))
        }
        rusqlite::types::ValueRef::Real(f) => {
            match Decimal::try_from(f) {
                Ok(decimal) => Ok(Some(decimal.serialize().to_vec())),
                Err(e) => Err(rusqlite::Error::UserFunctionError(format!("Cannot convert float to decimal: {}", e).into()))
            }
        }
        _ => Err(rusqlite::Error::UserFunctionError("Expected text, integer, or real value".into()))
    }
}

fn decimal_to_text(ctx: &Context<'_>) -> Result<Option<String>> {
    match ctx.get_raw(0) {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Blob(bytes) => {
            if bytes.len() == 16 {
                let mut array = [0u8; 16];
                array.copy_from_slice(bytes);
                let decimal = Decimal::deserialize(array);
                Ok(Some(decimal.to_string()))
            } else {
                Err(rusqlite::Error::UserFunctionError("Invalid decimal binary format".into()))
            }
        }
        rusqlite::types::ValueRef::Text(s) => {
            // Allow direct text input for compatibility
            let text = std::str::from_utf8(s).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            Ok(Some(text.to_string()))
        }
        _ => Err(rusqlite::Error::UserFunctionError("Expected blob or text value".into()))
    }
}

// Helper to get decimal from context
fn get_decimal(ctx: &Context<'_>, idx: usize) -> Result<Option<Decimal>> {
    match ctx.get_raw(idx) {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Blob(bytes) => {
            if bytes.len() == 16 {
                let mut array = [0u8; 16];
                array.copy_from_slice(bytes);
                Ok(Some(Decimal::deserialize(array)))
            } else {
                Err(rusqlite::Error::UserFunctionError("Invalid decimal binary format".into()))
            }
        }
        rusqlite::types::ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            Decimal::from_str(text)
                .map(Some)
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
        }
        rusqlite::types::ValueRef::Integer(i) => {
            Ok(Some(Decimal::new(i, 0)))
        }
        rusqlite::types::ValueRef::Real(f) => {
            Decimal::try_from(f)
                .map(Some)
                .map_err(|e| rusqlite::Error::UserFunctionError(format!("Cannot convert float to decimal: {}", e).into()))
        }
    }
}

// Arithmetic functions

fn decimal_add(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            let result = a + b;
            Ok(Some(result.serialize().to_vec()))
        }
        _ => Ok(None)
    }
}

fn decimal_sub(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            let result = a - b;
            Ok(Some(result.serialize().to_vec()))
        }
        _ => Ok(None)
    }
}

fn decimal_mul(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            let result = a * b;
            Ok(Some(result.serialize().to_vec()))
        }
        _ => Ok(None)
    }
}

fn decimal_div(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            if b.is_zero() {
                Err(rusqlite::Error::UserFunctionError("Division by zero".into()))
            } else {
                let result = a / b;
                Ok(Some(result.serialize().to_vec()))
            }
        }
        _ => Ok(None)
    }
}

// Comparison functions

fn decimal_eq(ctx: &Context<'_>) -> Result<bool> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => Ok(a == b),
        (None, None) => Ok(true),
        _ => Ok(false)
    }
}

fn decimal_lt(ctx: &Context<'_>) -> Result<bool> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => Ok(a < b),
        _ => Ok(false)
    }
}

fn decimal_gt(ctx: &Context<'_>) -> Result<bool> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => Ok(a > b),
        _ => Ok(false)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_decimal_arithmetic() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        register_decimal_functions(&conn)?;

        // Test basic arithmetic
        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_add(decimal_from_text('123.45'), decimal_from_text('67.89')))",
            [],
            |row| row.get(0)
        )?;
        assert_eq!(result, "191.34");

        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_sub(decimal_from_text('100.00'), decimal_from_text('25.50')))",
            [],
            |row| row.get(0)
        )?;
        assert_eq!(result, "74.50");

        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_mul(decimal_from_text('10.5'), decimal_from_text('2')))",
            [],
            |row| row.get(0)
        )?;
        assert_eq!(result, "21.0");

        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_div(decimal_from_text('100'), decimal_from_text('3')))",
            [],
            |row| row.get(0)
        )?;
        assert!(result.starts_with("33.333333"));

        Ok(())
    }

}