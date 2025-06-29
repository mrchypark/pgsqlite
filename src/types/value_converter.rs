use crate::types::type_mapper::PgType;
use std::net::{Ipv4Addr, Ipv6Addr};
use regex::Regex;

pub struct ValueConverter;

impl ValueConverter {
    /// Convert a PostgreSQL value to SQLite storage format
    pub fn pg_to_sqlite(value: &str, pg_type: PgType) -> Result<String, String> {
        match pg_type {
            PgType::Money => Self::convert_money(value),
            PgType::Int4range | PgType::Int8range | PgType::Numrange => Self::convert_range(value),
            PgType::Cidr => Self::convert_cidr(value),
            PgType::Inet => Self::convert_inet(value),
            PgType::Macaddr => Self::convert_macaddr(value),
            PgType::Macaddr8 => Self::convert_macaddr8(value),
            PgType::Bit | PgType::Varbit => Self::convert_bit(value),
            _ => Ok(value.to_string()), // Pass through other types
        }
    }
    
    /// Convert a SQLite value back to PostgreSQL format
    pub fn sqlite_to_pg(value: &str, pg_type: PgType) -> Result<String, String> {
        match pg_type {
            PgType::Money => Ok(value.to_string()), // Money is stored as-is
            PgType::Int4range | PgType::Int8range | PgType::Numrange => Ok(value.to_string()), // Ranges stored as-is
            PgType::Cidr => Ok(value.to_string()), // CIDR stored as-is
            PgType::Inet => Ok(value.to_string()), // INET stored as-is
            PgType::Macaddr => Ok(value.to_string()), // MAC addresses stored as-is
            PgType::Macaddr8 => Ok(value.to_string()),
            PgType::Bit | PgType::Varbit => Ok(value.to_string()), // Bit strings stored as-is
            _ => Ok(value.to_string()),
        }
    }
    
    /// Validate and convert money values
    fn convert_money(value: &str) -> Result<String, String> {
        // Remove whitespace
        let trimmed = value.trim();
        
        // Check for currency symbols and valid decimal format
        let money_regex = Regex::new(r"^[\$€£¥]?-?\d+(\.\d{1,2})?$|^-[\$€£¥]\d+(\.\d{1,2})?$").unwrap();
        if money_regex.is_match(trimmed) {
            Ok(trimmed.to_string())
        } else {
            Err(format!("Invalid money format: {}", value))
        }
    }
    
    /// Validate and convert range values
    fn convert_range(value: &str) -> Result<String, String> {
        // Range format: [lower,upper) or (lower,upper] or [lower,upper] or (lower,upper)
        let range_regex = Regex::new(r"^[\[\(]-?\d+,-?\d+[\]\)]$").unwrap();
        if range_regex.is_match(value.trim()) {
            Ok(value.trim().to_string())
        } else {
            Err(format!("Invalid range format: {}", value))
        }
    }
    
    /// Validate and convert CIDR values
    fn convert_cidr(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Split on '/'
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid CIDR format: {}", value));
        }
        
        let ip_part = parts[0];
        let prefix_part = parts[1];
        
        // Validate IP address
        if !Self::is_valid_ip(ip_part) {
            return Err(format!("Invalid IP address in CIDR: {}", ip_part));
        }
        
        // Validate prefix length
        let prefix: u8 = prefix_part.parse()
            .map_err(|_| format!("Invalid prefix length: {}", prefix_part))?;
        
        if ip_part.contains(':') {
            // IPv6
            if prefix > 128 {
                return Err(format!("IPv6 prefix length cannot exceed 128: {}", prefix));
            }
        } else {
            // IPv4
            if prefix > 32 {
                return Err(format!("IPv4 prefix length cannot exceed 32: {}", prefix));
            }
        }
        
        Ok(trimmed.to_string())
    }
    
    /// Validate and convert INET values
    fn convert_inet(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // INET can be just an IP address or IP/prefix
        if trimmed.contains('/') {
            Self::convert_cidr(trimmed)
        } else if Self::is_valid_ip(trimmed) {
            Ok(trimmed.to_string())
        } else {
            Err(format!("Invalid INET format: {}", value))
        }
    }
    
    /// Validate and convert MAC address (6 bytes)
    fn convert_macaddr(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Support colon and hyphen separators
        let normalized = if trimmed.contains(':') {
            trimmed.to_string()
        } else if trimmed.contains('-') {
            trimmed.replace('-', ":")
        } else {
            return Err(format!("Invalid MAC address format: {}", value));
        };
        
        let parts: Vec<&str> = normalized.split(':').collect();
        if parts.len() != 6 {
            return Err(format!("MAC address must have 6 parts: {}", value));
        }
        
        for part in &parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("Invalid MAC address part: {}", part));
            }
        }
        
        Ok(normalized)
    }
    
    /// Validate and convert MAC address (8 bytes)
    fn convert_macaddr8(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Support colon and hyphen separators
        let normalized = if trimmed.contains(':') {
            trimmed.to_string()
        } else if trimmed.contains('-') {
            trimmed.replace('-', ":")
        } else {
            return Err(format!("Invalid MAC address format: {}", value));
        };
        
        let parts: Vec<&str> = normalized.split(':').collect();
        if parts.len() != 8 {
            return Err(format!("MAC address must have 8 parts: {}", value));
        }
        
        for part in &parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("Invalid MAC address part: {}", part));
            }
        }
        
        Ok(normalized)
    }
    
    /// Validate and convert bit strings
    fn convert_bit(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Remove B prefix if present (e.g., B'1010')
        let bit_string = if trimmed.starts_with("B'") && trimmed.ends_with('\'') {
            &trimmed[2..trimmed.len()-1]
        } else {
            trimmed
        };
        
        // Validate all characters are 0 or 1
        if bit_string.chars().all(|c| c == '0' || c == '1') {
            Ok(bit_string.to_string())
        } else {
            Err(format!("Invalid bit string: {}", value))
        }
    }
    
    /// Check if a string is a valid IP address (IPv4 or IPv6)
    fn is_valid_ip(s: &str) -> bool {
        s.parse::<Ipv4Addr>().is_ok() || s.parse::<Ipv6Addr>().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_money_conversion() {
        assert!(ValueConverter::convert_money("$123.45").is_ok());
        assert!(ValueConverter::convert_money("€100.00").is_ok());
        assert!(ValueConverter::convert_money("£50.5").is_ok());
        assert!(ValueConverter::convert_money("-$25.99").is_ok());
        assert!(ValueConverter::convert_money("invalid").is_err());
    }
    
    #[test]
    fn test_cidr_conversion() {
        assert!(ValueConverter::convert_cidr("192.168.1.0/24").is_ok());
        assert!(ValueConverter::convert_cidr("10.0.0.0/8").is_ok());
        assert!(ValueConverter::convert_cidr("2001:db8::/32").is_ok());
        assert!(ValueConverter::convert_cidr("192.168.1.0/33").is_err()); // Invalid prefix
        assert!(ValueConverter::convert_cidr("invalid/24").is_err());
    }
    
    #[test]
    fn test_inet_conversion() {
        assert!(ValueConverter::convert_inet("192.168.1.1").is_ok());
        assert!(ValueConverter::convert_inet("192.168.1.0/24").is_ok());
        assert!(ValueConverter::convert_inet("2001:db8::1").is_ok());
        assert!(ValueConverter::convert_inet("invalid").is_err());
    }
    
    #[test]
    fn test_macaddr_conversion() {
        assert!(ValueConverter::convert_macaddr("08:00:2b:01:02:03").is_ok());
        assert!(ValueConverter::convert_macaddr("08-00-2b-01-02-03").is_ok());
        assert!(ValueConverter::convert_macaddr("08:00:2b:01:02").is_err()); // Too few parts
        assert!(ValueConverter::convert_macaddr("invalid").is_err());
    }
    
    #[test]
    fn test_bit_conversion() {
        assert!(ValueConverter::convert_bit("1010").is_ok());
        assert!(ValueConverter::convert_bit("B'1010'").is_ok());
        assert!(ValueConverter::convert_bit("1012").is_err()); // Invalid character
    }
}