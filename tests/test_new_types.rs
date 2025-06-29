use pgsqlite::types::{PgType, TypeMapper, ValueConverter};
use pgsqlite::translator::CreateTableTranslator;

#[test]
fn test_new_pg_type_oids() {
    // Test that all new types have correct OIDs
    assert_eq!(PgType::Money.to_oid(), 790);
    assert_eq!(PgType::Int4range.to_oid(), 3904);
    assert_eq!(PgType::Int8range.to_oid(), 3926);
    assert_eq!(PgType::Numrange.to_oid(), 3906);
    assert_eq!(PgType::Cidr.to_oid(), 650);
    assert_eq!(PgType::Inet.to_oid(), 869);
    assert_eq!(PgType::Macaddr.to_oid(), 829);
    assert_eq!(PgType::Macaddr8.to_oid(), 774);
    assert_eq!(PgType::Bit.to_oid(), 1560);
    assert_eq!(PgType::Varbit.to_oid(), 1562);
}

#[test]
fn test_new_pg_type_from_oid() {
    // Test reverse OID mapping
    assert_eq!(PgType::from_oid(790), Some(PgType::Money));
    assert_eq!(PgType::from_oid(3904), Some(PgType::Int4range));
    assert_eq!(PgType::from_oid(3926), Some(PgType::Int8range));
    assert_eq!(PgType::from_oid(3906), Some(PgType::Numrange));
    assert_eq!(PgType::from_oid(650), Some(PgType::Cidr));
    assert_eq!(PgType::from_oid(869), Some(PgType::Inet));
    assert_eq!(PgType::from_oid(829), Some(PgType::Macaddr));
    assert_eq!(PgType::from_oid(774), Some(PgType::Macaddr8));
    assert_eq!(PgType::from_oid(1560), Some(PgType::Bit));
    assert_eq!(PgType::from_oid(1562), Some(PgType::Varbit));
}

#[test]
fn test_new_pg_type_names() {
    // Test type name mapping
    assert_eq!(PgType::Money.name(), "money");
    assert_eq!(PgType::Int4range.name(), "int4range");
    assert_eq!(PgType::Int8range.name(), "int8range");
    assert_eq!(PgType::Numrange.name(), "numrange");
    assert_eq!(PgType::Cidr.name(), "cidr");
    assert_eq!(PgType::Inet.name(), "inet");
    assert_eq!(PgType::Macaddr.name(), "macaddr");
    assert_eq!(PgType::Macaddr8.name(), "macaddr8");
    assert_eq!(PgType::Bit.name(), "bit");
    assert_eq!(PgType::Varbit.name(), "varbit");
}

#[test]
fn test_type_mapper_new_types() {
    let mapper = TypeMapper::new();
    
    // Test PostgreSQL to SQLite mapping
    assert_eq!(mapper.pg_to_sqlite("money"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("int4range"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("int8range"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("numrange"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("cidr"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("inet"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("macaddr"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("macaddr8"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("bit"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("bit varying"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("varbit"), "TEXT");
}

#[test]
fn test_value_inference_new_types() {
    // Test value-based type inference
    assert_eq!(TypeMapper::infer_pg_type_from_value("$123.45"), PgType::Money);
    assert_eq!(TypeMapper::infer_pg_type_from_value("€100.00"), PgType::Money);
    assert_eq!(TypeMapper::infer_pg_type_from_value("£50.99"), PgType::Money);
    
    assert_eq!(TypeMapper::infer_pg_type_from_value("192.168.1.0/24"), PgType::Cidr);
    assert_eq!(TypeMapper::infer_pg_type_from_value("192.168.1.1"), PgType::Inet);
    assert_eq!(TypeMapper::infer_pg_type_from_value("2001:db8::1"), PgType::Inet);
    
    assert_eq!(TypeMapper::infer_pg_type_from_value("08:00:2b:01:02:03"), PgType::Macaddr);
    assert_eq!(TypeMapper::infer_pg_type_from_value("08:00:2b:01:02:03:04:05"), PgType::Macaddr8);
    
    assert_eq!(TypeMapper::infer_pg_type_from_value("10101010"), PgType::Bit);
    assert_eq!(TypeMapper::infer_pg_type_from_value("[1,10)"), PgType::Int4range);
}

#[test]
fn test_create_table_translator_new_types() {
    // Test CREATE TABLE translation with new types
    let test_cases = vec![
        (
            "CREATE TABLE test (price MONEY, network CIDR, mac MACADDR)",
            "CREATE TABLE test (price TEXT, network TEXT, mac TEXT)"
        ),
        (
            "CREATE TABLE ranges (int_range INT4RANGE, num_range NUMRANGE)",
            "CREATE TABLE ranges (int_range TEXT, num_range TEXT)"
        ),
        (
            "CREATE TABLE net (ip INET, mac8 MACADDR8, bits BIT(8))",
            "CREATE TABLE net (ip TEXT, mac8 TEXT, bits TEXT)"
        ),
    ];
    
    for (pg_sql, expected_sqlite) in test_cases {
        let (result, _) = CreateTableTranslator::translate(pg_sql).unwrap();
        assert_eq!(result, expected_sqlite);
    }
}

#[test]
fn test_value_converter_money() {
    // Test money value conversion
    assert!(ValueConverter::pg_to_sqlite("$123.45", PgType::Money).is_ok());
    assert!(ValueConverter::pg_to_sqlite("€100.00", PgType::Money).is_ok());
    assert!(ValueConverter::pg_to_sqlite("£50.99", PgType::Money).is_ok());
    assert!(ValueConverter::pg_to_sqlite("-$25.00", PgType::Money).is_ok());
    
    // Invalid money formats
    assert!(ValueConverter::pg_to_sqlite("$123.456", PgType::Money).is_err()); // Too many decimal places
    assert!(ValueConverter::pg_to_sqlite("invalid", PgType::Money).is_err());
}

#[test]
fn test_value_converter_ranges() {
    // Test range value conversion
    assert!(ValueConverter::pg_to_sqlite("[1,10)", PgType::Int4range).is_ok());
    assert!(ValueConverter::pg_to_sqlite("(1,10]", PgType::Int4range).is_ok());
    assert!(ValueConverter::pg_to_sqlite("[1,10]", PgType::Int4range).is_ok());
    assert!(ValueConverter::pg_to_sqlite("(-5,5)", PgType::Int4range).is_ok());
    
    // Invalid range formats
    assert!(ValueConverter::pg_to_sqlite("[1,10", PgType::Int4range).is_err()); // Missing closing bracket
    assert!(ValueConverter::pg_to_sqlite("1,10", PgType::Int4range).is_err()); // Missing brackets
}

#[test]
fn test_value_converter_network() {
    // Test CIDR conversion
    assert!(ValueConverter::pg_to_sqlite("192.168.1.0/24", PgType::Cidr).is_ok());
    assert!(ValueConverter::pg_to_sqlite("10.0.0.0/8", PgType::Cidr).is_ok());
    assert!(ValueConverter::pg_to_sqlite("2001:db8::/32", PgType::Cidr).is_ok());
    
    // Invalid CIDR
    assert!(ValueConverter::pg_to_sqlite("192.168.1.0/33", PgType::Cidr).is_err()); // Invalid prefix
    assert!(ValueConverter::pg_to_sqlite("invalid/24", PgType::Cidr).is_err());
    
    // Test INET conversion
    assert!(ValueConverter::pg_to_sqlite("192.168.1.1", PgType::Inet).is_ok());
    assert!(ValueConverter::pg_to_sqlite("192.168.1.0/24", PgType::Inet).is_ok());
    assert!(ValueConverter::pg_to_sqlite("2001:db8::1", PgType::Inet).is_ok());
    
    // Invalid INET
    assert!(ValueConverter::pg_to_sqlite("invalid", PgType::Inet).is_err());
}

#[test]
fn test_value_converter_mac() {
    // Test MAC address conversion
    assert!(ValueConverter::pg_to_sqlite("08:00:2b:01:02:03", PgType::Macaddr).is_ok());
    assert!(ValueConverter::pg_to_sqlite("08-00-2b-01-02-03", PgType::Macaddr).is_ok());
    
    // Test MAC8 address conversion
    assert!(ValueConverter::pg_to_sqlite("08:00:2b:01:02:03:04:05", PgType::Macaddr8).is_ok());
    assert!(ValueConverter::pg_to_sqlite("08-00-2b-01-02-03-04-05", PgType::Macaddr8).is_ok());
    
    // Invalid MAC addresses
    assert!(ValueConverter::pg_to_sqlite("08:00:2b:01:02", PgType::Macaddr).is_err()); // Too few parts
    assert!(ValueConverter::pg_to_sqlite("08:00:2b:01:02:03:04", PgType::Macaddr8).is_err()); // Too few parts for MAC8
    assert!(ValueConverter::pg_to_sqlite("invalid", PgType::Macaddr).is_err());
}

#[test]
fn test_value_converter_bit() {
    // Test bit string conversion
    assert!(ValueConverter::pg_to_sqlite("1010", PgType::Bit).is_ok());
    assert!(ValueConverter::pg_to_sqlite("B'1010'", PgType::Bit).is_ok());
    assert!(ValueConverter::pg_to_sqlite("0", PgType::Bit).is_ok());
    assert!(ValueConverter::pg_to_sqlite("1", PgType::Bit).is_ok());
    
    // Invalid bit strings
    assert!(ValueConverter::pg_to_sqlite("1012", PgType::Bit).is_err()); // Invalid character
    assert!(ValueConverter::pg_to_sqlite("abc", PgType::Bit).is_err());
}