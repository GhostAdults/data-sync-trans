/// Pipeline 消息映射器
/// 将数据库行转换为 PipelineMessage

use anyhow::Result;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

use crate::core::TypedVal;
use crate::util::RowMapper;

// 重新导出 pipeline 中的类型
pub use crate::core::pipeline::{PipelineMessage, MappedRow};

/// Pipeline 行映射器
pub struct PipelineRowMapper {
    column_mapping: BTreeMap<String, String>,
    column_types: Option<BTreeMap<String, String>>,
}

impl PipelineRowMapper {
    pub fn new(
        column_mapping: BTreeMap<String, String>,
        column_types: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self {
            column_mapping,
            column_types,
        }
    }

    /// 提取 JSON 路径
    fn extract_by_path<'a>(root: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
        if path == "/" || path.is_empty() {
            return Some(root);
        }
        if path.starts_with('/') {
            return root.pointer(path);
        }
        let mut cur = root;
        for seg in path.split('.') {
            match cur {
                JsonValue::Object(map) => {
                    cur = map.get(seg)?;
                }
                JsonValue::Array(arr) => {
                    if let Ok(idx) = seg.parse::<usize>() {
                        cur = arr.get(idx)?;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(cur)
    }

    /// 将 JSON 值转换为类型化的 SQL 值
    fn to_typed_value(v: &JsonValue, type_hint: Option<&str>) -> Result<TypedVal> {
        use anyhow::Context;

        match type_hint {
            Some("int") => {
                if v.is_null() {
                    Ok(TypedVal::OptI64(None))
                } else {
                    let n = v.as_i64().context("无法转换为 int")?;
                    Ok(TypedVal::I64(n))
                }
            }
            Some("float") => {
                if v.is_null() {
                    Ok(TypedVal::OptF64(None))
                } else {
                    let n = v.as_f64().context("无法转换为 float")?;
                    Ok(TypedVal::F64(n))
                }
            }
            Some("bool") => {
                if v.is_null() {
                    Ok(TypedVal::OptBool(None))
                } else {
                    let b = v.as_bool().context("无法转换为 bool")?;
                    Ok(TypedVal::Bool(b))
                }
            }
            Some("timestamp") => {
                if v.is_null() {
                    Ok(TypedVal::OptNaiveTs(None))
                } else {
                    let s = v.as_str().context("timestamp 需要字符串")?;
                    let ts = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .context("时间格式错误")?;
                    Ok(TypedVal::OptNaiveTs(Some(ts)))
                }
            }
            _ => {
                // 默认为 text
                let s = v.as_str().map(|s| s.to_string())
                    .or_else(|| Some(v.to_string()))
                    .unwrap();
                Ok(TypedVal::Text(s))
            }
        }
    }
}

impl RowMapper<PipelineMessage> for PipelineRowMapper {
    fn map_rows(&self, rows: &[JsonValue]) -> Result<Vec<PipelineMessage>> {
        use std::collections::HashMap;

        let mut mapped_rows = Vec::new();

        for item in rows {
            let mut row = HashMap::new();

            for (target_col, source_path) in &self.column_mapping {
                let source_val = Self::extract_by_path(item, source_path)
                    .unwrap_or(&JsonValue::Null);

                let type_hint = self
                    .column_types
                    .as_ref()
                    .and_then(|m| m.get(target_col))
                    .map(|s| s.as_str());

                let typed_val = Self::to_typed_value(source_val, type_hint)?;
                row.insert(target_col.clone(), typed_val);
            }

            mapped_rows.push(MappedRow {
                values: row,
                source: item.clone(),
            });
        }

        // 包装为 PipelineMessage::DataBatch
        Ok(vec![PipelineMessage::DataBatch(mapped_rows)])
    }
}
