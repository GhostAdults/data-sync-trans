//! Schema 演进检查实现

use tracing::{info, warn};

use super::{ColumnInfo, CompatibilityResult, SchemaChange, TableSchema};

/// Schema 演进检查器
pub struct EvolutionChecker {
    /// 是否在检测到破坏性变更时中断
    break_on_incompatible: bool,
}

impl EvolutionChecker {
    pub fn new(break_on_incompatible: bool) -> Self {
        Self {
            break_on_incompatible,
        }
    }

    /// 检查 Schema 兼容性
    pub fn check(&self, old: Option<&TableSchema>, new: &TableSchema) -> CompatibilityResult {
        match old {
            None => {
                info!(
                    "No previous schema found for {}, treating as compatible",
                    new.table_name
                );
                CompatibilityResult::Compatible
            }
            Some(old_schema) => self.compare_schemas(old_schema, new),
        }
    }

    /// 比较两个 Schema 的兼容性
    fn compare_schemas(&self, old: &TableSchema, new: &TableSchema) -> CompatibilityResult {
        let changes = self.detect_all_changes(old, new);

        if changes.is_empty() {
            return CompatibilityResult::Compatible;
        }

        let mut breaking_changes = Vec::new();
        let mut forward_compatible = Vec::new();
        let mut backward_compatible = Vec::new();

        for change in &changes {
            match change {
                // 新增可空列 - 向前兼容（新版本可读旧数据）
                SchemaChange::ColumnAdded(col) if col.nullable => {
                    forward_compatible.push(change.clone());
                }
                // 新增非空列 - 破坏性（旧数据没有该列）
                SchemaChange::ColumnAdded(col) => {
                    breaking_changes.push(format!("Non-nullable column '{}' added", col.name));
                }
                // 删除列 - 破坏性
                SchemaChange::ColumnRemoved(name) => {
                    breaking_changes.push(format!("Column '{}' removed", name));
                }
                // 类型变更 - 通常破坏性
                SchemaChange::ColumnTypeChanged {
                    column_name,
                    old_type,
                    new_type,
                } => {
                    if self.is_type_compatible(old_type, new_type) {
                        forward_compatible.push(change.clone());
                    } else {
                        breaking_changes.push(format!(
                            "Column '{}' type changed from {} to {}",
                            column_name, old_type, new_type
                        ));
                    }
                }
                // 可空性变更
                SchemaChange::NullabilityChanged {
                    column_name,
                    old_nullable,
                    new_nullable,
                } => {
                    if *old_nullable && !new_nullable {
                        // 从可空变为非空 - 破坏性（可能有 NULL 值）
                        breaking_changes.push(format!(
                            "Column '{}' changed from nullable to non-nullable",
                            column_name
                        ));
                    } else {
                        // 从非空变为可空 - 向后兼容
                        backward_compatible.push(change.clone());
                    }
                }
                // 无变更
                SchemaChange::NoChange => {}
            }
        }

        // 决定最终兼容性
        if !breaking_changes.is_empty() {
            let reason = format!("Breaking changes detected: {}", breaking_changes.join(", "));

            if self.break_on_incompatible {
                warn!("{}", reason);
            }

            return CompatibilityResult::Breaking { reason, changes };
        }

        if !forward_compatible.is_empty() && backward_compatible.is_empty() {
            return CompatibilityResult::ForwardCompatible {
                changes: forward_compatible,
            };
        }

        if !backward_compatible.is_empty() && forward_compatible.is_empty() {
            return CompatibilityResult::BackwardCompatible {
                changes: backward_compatible,
            };
        }

        // 两种变更都有
        if !forward_compatible.is_empty() {
            return CompatibilityResult::ForwardCompatible {
                changes: forward_compatible,
            };
        }

        CompatibilityResult::Compatible
    }

    /// 检测所有变更
    fn detect_all_changes(&self, old: &TableSchema, new: &TableSchema) -> Vec<SchemaChange> {
        let mut changes = Vec::new();

        let old_columns: std::collections::HashMap<&str, &ColumnInfo> =
            old.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        let new_columns: std::collections::HashMap<&str, &ColumnInfo> =
            new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        // 新增列
        for (name, col) in &new_columns {
            if !old_columns.contains_key(*name) {
                changes.push(SchemaChange::ColumnAdded((*col).clone()));
            }
        }

        // 删除列
        for name in old_columns.keys() {
            if !new_columns.contains_key(*name) {
                changes.push(SchemaChange::ColumnRemoved((*name).to_string()));
            }
        }

        // 类型变更和可空性变更
        for (name, new_col) in &new_columns {
            if let Some(old_col) = old_columns.get(*name) {
                if old_col.logical_type != new_col.logical_type {
                    changes.push(SchemaChange::ColumnTypeChanged {
                        column_name: (*name).to_string(),
                        old_type: old_col.logical_type.clone(),
                        new_type: new_col.logical_type.clone(),
                    });
                }

                if old_col.nullable != new_col.nullable {
                    changes.push(SchemaChange::NullabilityChanged {
                        column_name: (*name).to_string(),
                        old_nullable: old_col.nullable,
                        new_nullable: new_col.nullable,
                    });
                }
            }
        }

        changes
    }

    /// 检查类型是否兼容
    fn is_type_compatible(&self, old_type: &str, new_type: &str) -> bool {
        // 类型扩展（如 int -> bigint）通常兼容
        // 类型缩小（如 bigint -> int）通常不兼容
        match (old_type, new_type) {
            // 同类型
            (a, b) if a == b => true,
            // 整数扩展
            ("int", "int") => true,
            // 浮点扩展
            ("float", "float") => true,
            // 其他情况视为不兼容
            _ => false,
        }
    }
}

impl Default for EvolutionChecker {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_column(name: &str, logical_type: &str, nullable: bool) -> ColumnInfo {
        let mut col = ColumnInfo::new(
            name.to_string(),
            logical_type.to_string(),
            logical_type.to_string(),
        );
        col.nullable = nullable;
        col
    }

    fn create_test_schema(columns: Vec<(&str, &str, bool)>) -> TableSchema {
        let cols: Vec<ColumnInfo> = columns
            .into_iter()
            .enumerate()
            .map(|(i, (name, logical_type, nullable))| {
                let mut col = create_test_column(name, logical_type, nullable);
                col.ordinal = i as u32;
                col
            })
            .collect();

        TableSchema::new("test".to_string(), "postgres".to_string()).with_columns(cols)
    }

    #[test]
    fn test_compatible_no_changes() {
        let checker = EvolutionChecker::new(true);
        let old = create_test_schema(vec![("id", "int", false), ("name", "text", true)]);
        let new = create_test_schema(vec![("id", "int", false), ("name", "text", true)]);

        let result = checker.check(Some(&old), &new);
        assert!(matches!(result, CompatibilityResult::Compatible));
    }

    #[test]
    fn test_forward_compatible_add_nullable_column() {
        let checker = EvolutionChecker::new(true);
        let old = create_test_schema(vec![("id", "int", false)]);
        let new = create_test_schema(vec![("id", "int", false), ("email", "text", true)]);

        let result = checker.check(Some(&old), &new);
        assert!(matches!(
            result,
            CompatibilityResult::ForwardCompatible { .. }
        ));
    }

    #[test]
    fn test_breaking_remove_column() {
        let checker = EvolutionChecker::new(true);
        let old = create_test_schema(vec![("id", "int", false), ("name", "text", true)]);
        let new = create_test_schema(vec![("id", "int", false)]);

        let result = checker.check(Some(&old), &new);
        assert!(matches!(result, CompatibilityResult::Breaking { .. }));
    }

    #[test]
    fn test_breaking_type_change() {
        let checker = EvolutionChecker::new(true);
        let old = create_test_schema(vec![("id", "int", false), ("value", "int", true)]);
        let new = create_test_schema(vec![("id", "int", false), ("value", "text", true)]);

        let result = checker.check(Some(&old), &new);
        assert!(matches!(result, CompatibilityResult::Breaking { .. }));
    }

    #[test]
    fn test_new_schema_compatible() {
        let checker = EvolutionChecker::new(true);
        let new = create_test_schema(vec![("id", "int", false)]);

        let result = checker.check(None, &new);
        assert!(matches!(result, CompatibilityResult::Compatible));
    }
}
