//! WriteMode 单元测试

use data_trans_common::interface::WriteMode;

#[test]
fn test_write_mode_from_str_insert() {
    let mode = WriteMode::from_str("insert");
    assert_eq!(mode, WriteMode::Insert);
}

#[test]
fn test_write_mode_from_str_insert_uppercase() {
    let mode = WriteMode::from_str("INSERT");
    assert_eq!(mode, WriteMode::Insert);
}

#[test]
fn test_write_mode_from_str_insert_mixed_case() {
    let mode = WriteMode::from_str("InSeRt");
    assert_eq!(mode, WriteMode::Insert);
}

#[test]
fn test_write_mode_from_str_upsert() {
    let mode = WriteMode::from_str("upsert");
    assert_eq!(mode, WriteMode::Upsert);
}

#[test]
fn test_write_mode_from_str_upsert_uppercase() {
    let mode = WriteMode::from_str("UPSERT");
    assert_eq!(mode, WriteMode::Upsert);
}

#[test]
fn test_write_mode_from_str_invalid_defaults_to_insert() {
    let mode = WriteMode::from_str("invalid");
    assert_eq!(mode, WriteMode::Insert);
}

#[test]
fn test_write_mode_from_str_empty_defaults_to_insert() {
    let mode = WriteMode::from_str("");
    assert_eq!(mode, WriteMode::Insert);
}

#[test]
fn test_write_mode_as_str_insert() {
    assert_eq!(WriteMode::Insert.as_str(), "insert");
}

#[test]
fn test_write_mode_as_str_upsert() {
    assert_eq!(WriteMode::Upsert.as_str(), "upsert");
}

#[test]
fn test_write_mode_equality() {
    assert_eq!(WriteMode::Insert, WriteMode::Insert);
    assert_eq!(WriteMode::Upsert, WriteMode::Upsert);
    assert_ne!(WriteMode::Insert, WriteMode::Upsert);
}
