//! RDBMS 写入 SQL 构建与执行
//!
//! 提供按 WriteMode + DatabaseKind 生成正确 SQL 的函数组，
//! 以及批量数据准备和执行写入的能力。

use anyhow::{bail, Result};
use std::marker::PhantomData;
use std::sync::Arc;

use relus_common::job_config::WriteMode;
use relus_common::types::UnifiedValue;

use super::pool::{DatabaseKind, RdbmsPool};

#[derive(Debug, Clone, Copy)]
struct PostgresBackend;

#[derive(Debug, Clone, Copy)]
struct MysqlBackend;

trait SqlBackend {
    const REQUIRES_UPSERT_KEYS: bool;

    fn identifier(identifier: &str) -> String;
    fn column_list(columns: &str) -> String;
    fn push_identifier(out: &mut String, ident: &str);
    fn push_placeholder(out: &mut String, bind_index: &mut usize);
}

impl SqlBackend for PostgresBackend {
    const REQUIRES_UPSERT_KEYS: bool = true;

    fn identifier(identifier: &str) -> String {
        identifier
            .split('.')
            .map(|part| {
                if part.starts_with('"') && part.ends_with('"') {
                    part.to_string()
                } else {
                    let mut out = String::new();
                    Self::push_identifier(&mut out, part);
                    out
                }
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    fn column_list(columns: &str) -> String {
        columns
            .split(',')
            .map(|column| postgres_column_expr(column.trim()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn push_identifier(out: &mut String, ident: &str) {
        out.push('"');
        out.push_str(&ident.replace('"', "\"\""));
        out.push('"');
    }

    fn push_placeholder(out: &mut String, bind_index: &mut usize) {
        *bind_index += 1;
        out.push('$');
        out.push_str(&bind_index.to_string());
    }
}

impl SqlBackend for MysqlBackend {
    const REQUIRES_UPSERT_KEYS: bool = false;

    fn identifier(identifier: &str) -> String {
        identifier.to_string()
    }

    fn column_list(columns: &str) -> String {
        columns.to_string()
    }

    fn push_identifier(out: &mut String, ident: &str) {
        out.push_str(ident);
    }

    fn push_placeholder(out: &mut String, _bind_index: &mut usize) {
        out.push('?');
    }
}

fn postgres_column_expr(column: &str) -> String {
    if column == "*"
        || column.contains('(')
        || column.contains(')')
        || column.contains('"')
        || column.split_whitespace().count() > 1
    {
        column.to_string()
    } else {
        PostgresBackend::identifier(column)
    }
}

macro_rules! dispatch_backend {
    ($kind:expr, |$backend:ident| $body:expr) => {{
        match $kind {
            DatabaseKind::Postgres => {
                type $backend = PostgresBackend;
                $body
            }
            DatabaseKind::Mysql => {
                type $backend = MysqlBackend;
                $body
            }
        }
    }};
}

/// Lightweight SQL builder for read-side query construction.
#[derive(Debug, Clone, Copy)]
pub struct SqlBuilder {
    kind: DatabaseKind,
}

#[derive(Debug, Clone, Copy)]
pub struct PkRangeSelect<'a> {
    pub table: &'a str,
    pub columns: &'a str,
    pub where_clause: Option<&'a str>,
    pub pk: &'a str,
    pub min: &'a str,
    pub max: &'a str,
    pub inclusive_end: bool,
}

impl SqlBuilder {
    pub fn new(kind: DatabaseKind) -> Self {
        Self { kind }
    }

    pub fn select(&self, columns: &str, table: &str) -> SelectBuilder {
        dispatch_backend!(self.kind, |DB| SelectSqlBuilder::<DB>::select(
            columns, table
        ))
    }

    pub fn custom_query(&self, query: &str) -> SelectBuilder {
        SelectBuilder::from_raw(query)
    }

    pub fn count(&self, table: &str, custom_query: Option<&str>) -> String {
        match custom_query.filter(|query| !query.trim().is_empty()) {
            Some(query) => format!("SELECT COUNT(*) FROM ({}) AS subquery", query),
            None => format!("SELECT COUNT(*) FROM {}", table),
        }
    }

    pub fn pk_min_max(&self, table: &str, pk: &str, where_clause: Option<&str>) -> String {
        dispatch_backend!(self.kind, |DB| {
            SelectSqlBuilder::<DB>::pk_min_max(table, pk, where_clause)
        })
    }

    pub fn pk_range_select(&self, range: PkRangeSelect<'_>) -> String {
        dispatch_backend!(self.kind, |DB| SelectSqlBuilder::<DB>::pk_range_select(
            range
        ))
    }

    pub fn null_pk_select(
        &self,
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        pk: &str,
    ) -> String {
        dispatch_backend!(self.kind, |DB| {
            SelectSqlBuilder::<DB>::null_pk_select(table, columns, where_clause, pk)
        })
    }

    pub fn limit_offset_select(
        &self,
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> String {
        dispatch_backend!(self.kind, |DB| {
            SelectSqlBuilder::<DB>::limit_offset_select(table, columns, where_clause, limit, offset)
        })
    }

    pub fn literal_string(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub fn column_list(&self, columns: &str) -> String {
        dispatch_backend!(self.kind, |DB| DB::column_list(columns))
    }
}

struct SelectSqlBuilder<DB> {
    _backend: PhantomData<DB>,
}

impl<DB: SqlBackend> SelectSqlBuilder<DB> {
    fn select(columns: &str, table: &str) -> SelectBuilder {
        SelectBuilder::from_table(DB::column_list(columns), table)
    }

    fn pk_min_max(table: &str, pk: &str, where_clause: Option<&str>) -> String {
        let pk = DB::identifier(pk);
        Self::select(&format!("MIN({}), MAX({})", pk, pk), table)
            .where_raw(format!("{} IS NOT NULL", pk))
            .where_raw_opt(where_clause)
            .build()
    }

    fn pk_range_select(range: PkRangeSelect<'_>) -> String {
        let pk = DB::identifier(range.pk);
        let min = SqlBuilder::literal_string(range.min);
        let max = SqlBuilder::literal_string(range.max);
        let end_op = if range.inclusive_end { "<=" } else { "<" };
        let range_condition = format!("{} >= {} AND {} {} {}", pk, min, pk, end_op, max);

        Self::select(range.columns, range.table)
            .where_raw_opt(range.where_clause)
            .where_raw(range_condition)
            .build()
    }

    fn null_pk_select(table: &str, columns: &str, where_clause: Option<&str>, pk: &str) -> String {
        let pk = DB::identifier(pk);
        Self::select(columns, table)
            .where_raw_opt(where_clause)
            .where_raw(format!("{} IS NULL", pk))
            .build()
    }

    fn limit_offset_select(
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> String {
        Self::select(columns, table)
            .where_raw_opt(where_clause)
            .limit_offset(limit, offset)
            .build()
    }
}

#[derive(Debug, Clone)]
pub struct SelectBuilder {
    source: SelectSource,
    wheres: Vec<String>,
    limit_offset: Option<(usize, usize)>,
}

#[derive(Debug, Clone)]
enum SelectSource {
    Table { columns: String, table: String },
    Raw(String),
}

impl SelectBuilder {
    fn from_table(columns: String, table: &str) -> Self {
        Self {
            source: SelectSource::Table {
                columns,
                table: table.to_string(),
            },
            wheres: Vec::new(),
            limit_offset: None,
        }
    }

    fn from_raw(query: &str) -> Self {
        Self {
            source: SelectSource::Raw(query.trim().to_string()),
            wheres: Vec::new(),
            limit_offset: None,
        }
    }

    pub fn where_raw(mut self, condition: impl Into<String>) -> Self {
        let condition = condition.into();
        if !condition.trim().is_empty() {
            self.wheres.push(condition);
        }
        self
    }

    pub fn where_raw_opt(self, condition: Option<&str>) -> Self {
        match condition {
            Some(condition) => self.where_raw(condition),
            None => self,
        }
    }

    pub fn limit_offset(mut self, limit: usize, offset: usize) -> Self {
        self.limit_offset = Some((limit, offset));
        self
    }

    pub fn build(self) -> String {
        let mut sql = match self.source {
            SelectSource::Table { columns, table } => format!("SELECT {} FROM {}", columns, table),
            SelectSource::Raw(query) => query,
        };

        if !self.wheres.is_empty() {
            let conditions = self
                .wheres
                .iter()
                .map(|condition| format!("({})", condition))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(" WHERE ");
            sql.push_str(&conditions);
        }

        if let Some((limit, offset)) = self.limit_offset {
            sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));
        }

        sql
    }
}

/// 写入批次数据
pub struct WriteBatch {
    /// SQL 前缀（INSERT INTO t (cols) / UPDATE t SET / DELETE FROM t）
    pub sql: String,
    /// upsert 模式的 VALUES 后缀，其他模式为 None
    pub upsert_suffix: Option<String>,
    pub table_name: String,
    pub rows: Vec<Vec<UnifiedValue>>,
    pub columns: Vec<String>,
    pub key_columns: Vec<String>,
    pub mode: WriteMode,
}

/// SQL text plus flattened bind values prepared by the SQL builder.
pub struct BuiltWriteQuery {
    pub sql: String,
    pub params: Vec<UnifiedValue>,
}

impl WriteBatch {
    pub fn build_values_query(
        &self,
        rows: &[Vec<UnifiedValue>],
        kind: DatabaseKind,
    ) -> Result<BuiltWriteQuery> {
        ensure_row_width(rows, self.columns.len())?;
        let suffix = self.upsert_suffix.as_deref().unwrap_or("");
        build_values_query(&self.sql, rows, suffix, kind)
    }

    pub fn build_row_query(
        &self,
        row: &[UnifiedValue],
        kind: DatabaseKind,
    ) -> Result<BuiltWriteQuery> {
        ensure_row_width_for_row(row, self.columns.len())?;
        match self.mode {
            WriteMode::Insert | WriteMode::Upsert => {
                let rows = vec![row.to_vec()];
                self.build_values_query(&rows, kind)
            }
            WriteMode::Update => build_update_query(
                &self.table_name,
                &self.columns,
                row,
                &self.key_columns,
                kind,
            ),
            WriteMode::Delete => build_delete_query(
                &self.table_name,
                &self.columns,
                row,
                &self.key_columns,
                kind,
            ),
        }
    }
}

/// Column/value rows extracted from mapped pipeline rows.
pub struct WriteRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<UnifiedValue>>,
}

impl WriteRows {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<UnifiedValue>>) -> Self {
        Self { columns, rows }
    }

    pub fn table(self, table: impl Into<String>) -> WriteBatchBuilder {
        WriteBatchBuilder {
            rows: self,
            table_name: table.into(),
            key_columns: Vec::new(),
            mode: WriteMode::Insert,
            kind: DatabaseKind::Mysql,
        }
    }
}

/// Chainable builder that turns extracted write rows into executable SQL batch metadata.
pub struct WriteBatchBuilder {
    rows: WriteRows,
    table_name: String,
    key_columns: Vec<String>,
    mode: WriteMode,
    kind: DatabaseKind,
}

impl WriteBatchBuilder {
    pub fn key_columns(mut self, key_columns: &[String]) -> Self {
        self.key_columns = key_columns.to_vec();
        self
    }

    pub fn mode(mut self, mode: WriteMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn database_kind(mut self, kind: DatabaseKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn build(self) -> Result<WriteBatch> {
        if self.rows.rows.is_empty() {
            bail!("没有数据需要同步");
        }

        let (keys, nonkeys) = split_keys_nonkeys(&self.rows.columns, &self.key_columns);

        let (sql, upsert_suffix) = match self.mode {
            WriteMode::Insert => (
                build_insert_sql(&self.table_name, &self.rows.columns, self.kind),
                None,
            ),
            WriteMode::Upsert => {
                let parts = build_upsert_sql(
                    &self.table_name,
                    &self.rows.columns,
                    &keys,
                    &nonkeys,
                    self.kind,
                )?;
                (parts.prefix, Some(parts.suffix))
            }
            WriteMode::Update => (
                build_update_sql(&self.table_name, &nonkeys, &keys, self.kind)?,
                None,
            ),
            WriteMode::Delete => (build_delete_sql(&self.table_name, &keys, self.kind)?, None),
        };

        Ok(WriteBatch {
            sql,
            upsert_suffix,
            table_name: self.table_name,
            rows: self.rows.rows,
            columns: self.rows.columns,
            key_columns: self.key_columns,
            mode: self.mode,
        })
    }
}

#[derive(Debug)]
struct BuildCtx<DB> {
    sql: String,
    params: Vec<UnifiedValue>,
    bind_index: usize,
    _backend: PhantomData<DB>,
}

impl<DB: SqlBackend> BuildCtx<DB> {
    fn new() -> Self {
        Self {
            sql: String::new(),
            params: Vec::new(),
            bind_index: 0,
            _backend: PhantomData,
        }
    }

    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_ident(&mut self, ident: &str) {
        DB::push_identifier(&mut self.sql, ident);
    }

    fn push_table(&mut self, table: &str) {
        self.sql.push_str(table);
    }

    fn push_placeholder(&mut self) {
        DB::push_placeholder(&mut self.sql, &mut self.bind_index);
    }

    fn push_bind(&mut self, value: UnifiedValue) {
        self.push_placeholder();
        self.params.push(value);
    }

    fn finish(self) -> String {
        self.sql
    }

    fn finish_query(self) -> BuiltWriteQuery {
        BuiltWriteQuery {
            sql: self.sql,
            params: self.params,
        }
    }
}

trait QueryFragment<DB: SqlBackend> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>);
}

struct ColumnList<'a> {
    columns: &'a [String],
}

impl<DB: SqlBackend> QueryFragment<DB> for ColumnList<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                out.push_sql(", ");
            }
            out.push_ident(column);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AssignmentValue {
    Placeholder,
    Excluded,
    Values,
}

struct Assignments<'a> {
    columns: &'a [String],
    value: AssignmentValue,
}

impl<DB: SqlBackend> QueryFragment<DB> for Assignments<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                out.push_sql(", ");
            }

            out.push_ident(column);
            out.push_sql(" = ");

            match self.value {
                AssignmentValue::Placeholder => out.push_sql("?"),
                AssignmentValue::Excluded => {
                    out.push_sql("EXCLUDED.");
                    out.push_ident(column);
                }
                AssignmentValue::Values => {
                    out.push_sql("VALUES(");
                    out.push_ident(column);
                    out.push_sql(")");
                }
            }
        }
    }
}

struct WhereKeys<'a> {
    keys: &'a [String],
}

impl<DB: SqlBackend> QueryFragment<DB> for WhereKeys<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        for (i, key) in self.keys.iter().enumerate() {
            if i > 0 {
                out.push_sql(" AND ");
            }

            out.push_ident(key);
            out.push_sql(" = ?");
        }
    }
}

struct InsertStatement<'a> {
    table: &'a str,
    columns: &'a [String],
}

impl<DB: SqlBackend> QueryFragment<DB> for InsertStatement<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        out.push_sql("INSERT INTO ");
        out.push_table(self.table);
        out.push_sql(" (");
        ColumnList {
            columns: self.columns,
        }
        .walk_ast(out);
        out.push_sql(")");
    }
}

struct ConflictFragment<'a, DB> {
    keys: &'a [String],
    nonkeys: &'a [String],
    _backend: PhantomData<DB>,
}

impl QueryFragment<PostgresBackend> for ConflictFragment<'_, PostgresBackend> {
    fn walk_ast(&self, out: &mut BuildCtx<PostgresBackend>) {
        out.push_sql(" ON CONFLICT (");
        ColumnList { columns: self.keys }.walk_ast(out);
        out.push_sql(") DO UPDATE SET ");
        Assignments {
            columns: self.nonkeys,
            value: AssignmentValue::Excluded,
        }
        .walk_ast(out);
    }
}

impl QueryFragment<MysqlBackend> for ConflictFragment<'_, MysqlBackend> {
    fn walk_ast(&self, out: &mut BuildCtx<MysqlBackend>) {
        out.push_sql(" ON DUPLICATE KEY UPDATE ");
        Assignments {
            columns: self.nonkeys,
            value: AssignmentValue::Values,
        }
        .walk_ast(out);
    }
}

struct UpdateStatement<'a> {
    table: &'a str,
    nonkeys: &'a [String],
    keys: &'a [String],
}

impl<DB: SqlBackend> QueryFragment<DB> for UpdateStatement<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        out.push_sql("UPDATE ");
        out.push_table(self.table);
        out.push_sql(" SET ");
        Assignments {
            columns: self.nonkeys,
            value: AssignmentValue::Placeholder,
        }
        .walk_ast(out);
        out.push_sql(" WHERE ");
        WhereKeys { keys: self.keys }.walk_ast(out);
    }
}

struct DeleteStatement<'a> {
    table: &'a str,
    keys: &'a [String],
}

impl<DB: SqlBackend> QueryFragment<DB> for DeleteStatement<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        out.push_sql("DELETE FROM ");
        out.push_table(self.table);
        out.push_sql(" WHERE ");
        WhereKeys { keys: self.keys }.walk_ast(out);
    }
}

struct AssignmentsWithBinds<'a> {
    columns: &'a [String],
    values: &'a [UnifiedValue],
}

impl<DB: SqlBackend> QueryFragment<DB> for AssignmentsWithBinds<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        for (i, (column, value)) in self.columns.iter().zip(self.values).enumerate() {
            if i > 0 {
                out.push_sql(", ");
            }

            out.push_ident(column);
            out.push_sql(" = ");
            out.push_bind(value.clone());
        }
    }
}

struct WhereKeysWithBinds<'a> {
    keys: &'a [String],
    values: &'a [UnifiedValue],
}

impl<DB: SqlBackend> QueryFragment<DB> for WhereKeysWithBinds<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        for (i, (key, value)) in self.keys.iter().zip(self.values).enumerate() {
            if i > 0 {
                out.push_sql(" AND ");
            }

            out.push_ident(key);
            out.push_sql(" = ");
            out.push_bind(value.clone());
        }
    }
}

struct UpdateValuesStatement<'a> {
    table: &'a str,
    nonkeys: &'a [String],
    nonkey_values: &'a [UnifiedValue],
    keys: &'a [String],
    key_values: &'a [UnifiedValue],
}

impl<DB: SqlBackend> QueryFragment<DB> for UpdateValuesStatement<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        out.push_sql("UPDATE ");
        out.push_table(self.table);
        out.push_sql(" SET ");
        AssignmentsWithBinds {
            columns: self.nonkeys,
            values: self.nonkey_values,
        }
        .walk_ast(out);
        out.push_sql(" WHERE ");
        WhereKeysWithBinds {
            keys: self.keys,
            values: self.key_values,
        }
        .walk_ast(out);
    }
}

struct DeleteValuesStatement<'a> {
    table: &'a str,
    keys: &'a [String],
    key_values: &'a [UnifiedValue],
}

impl<DB: SqlBackend> QueryFragment<DB> for DeleteValuesStatement<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        out.push_sql("DELETE FROM ");
        out.push_table(self.table);
        out.push_sql(" WHERE ");
        WhereKeysWithBinds {
            keys: self.keys,
            values: self.key_values,
        }
        .walk_ast(out);
    }
}

struct BatchValuesStatement<'a> {
    base_sql: &'a str,
    rows: &'a [Vec<UnifiedValue>],
    suffix: &'a str,
}

impl<DB: SqlBackend> QueryFragment<DB> for BatchValuesStatement<'_> {
    fn walk_ast(&self, out: &mut BuildCtx<DB>) {
        out.push_sql(self.base_sql);
        out.push_sql(" VALUES ");

        for (row_index, row) in self.rows.iter().enumerate() {
            if row_index > 0 {
                out.push_sql(", ");
            }

            out.push_sql("(");
            for (col_index, value) in row.iter().enumerate() {
                if col_index > 0 {
                    out.push_sql(", ");
                }
                out.push_bind(value.clone());
            }
            out.push_sql(")");
        }

        out.push_sql(self.suffix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_quotes_postgres_columns_only() {
        let sql = SqlBuilder::new(DatabaseKind::Postgres)
            .select("id, name", "users")
            .build();
        println!("Generated SQL: {}", sql);
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM users");
    }

    #[test]
    fn select_keeps_mysql_columns() {
        let sql = SqlBuilder::new(DatabaseKind::Mysql)
            .select("id, name", "users")
            .build();

        assert_eq!(sql, "SELECT id, name FROM users");
    }

    #[test]
    fn count_supports_table_and_custom_query() {
        let builder = SqlBuilder::new(DatabaseKind::Mysql);

        assert_eq!(builder.count("users", None), "SELECT COUNT(*) FROM users");
        assert_eq!(
            builder.count("users", Some("SELECT id FROM users WHERE active = 1")),
            "SELECT COUNT(*) FROM (SELECT id FROM users WHERE active = 1) AS subquery"
        );
    }

    #[test]
    fn range_query_combines_where_clause() {
        let sql = SqlBuilder::new(DatabaseKind::Postgres).pk_range_select(PkRangeSelect {
            table: "users",
            columns: "id, name",
            where_clause: Some("active = true"),
            pk: "id",
            min: "a",
            max: "b",
            inclusive_end: false,
        });

        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM users WHERE (active = true) AND (\"id\" >= 'a' AND \"id\" < 'b')"
        );
    }

    #[test]
    fn null_pk_query_combines_where_clause() {
        let sql = SqlBuilder::new(DatabaseKind::Mysql).null_pk_select(
            "users",
            "id, name",
            Some("active = 1"),
            "id",
        );

        assert_eq!(
            sql,
            "SELECT id, name FROM users WHERE (active = 1) AND (id IS NULL)"
        );
    }

    #[test]
    fn limit_offset_query() {
        let sql = SqlBuilder::new(DatabaseKind::Mysql).limit_offset_select(
            "users",
            "id, name",
            Some("active = 1"),
            20,
            40,
        );

        assert_eq!(
            sql,
            "SELECT id, name FROM users WHERE (active = 1) LIMIT 20 OFFSET 40"
        );
    }

    #[test]
    fn literal_string_escapes_single_quote() {
        assert_eq!(SqlBuilder::literal_string("O'Reilly"), "'O''Reilly'");
    }

    #[test]
    fn write_rows_builds_insert_batch_through_chain() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ]];

        let batch = WriteRows::new(columns, rows)
            .table("users")
            .mode(WriteMode::Insert)
            .database_kind(DatabaseKind::Postgres)
            .build()
            .unwrap();

        assert_eq!(batch.sql, "INSERT INTO users (\"id\", \"name\")");
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.columns, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn insert_sql_keeps_existing_dialect_output() {
        let columns = vec!["id".to_string(), "name".to_string()];

        assert_eq!(
            build_insert_sql("users", &columns, DatabaseKind::Postgres),
            "INSERT INTO users (\"id\", \"name\")"
        );
        assert_eq!(
            build_insert_sql("users", &columns, DatabaseKind::Mysql),
            "INSERT INTO users (id, name)"
        );
    }

    #[test]
    fn upsert_sql_keeps_existing_postgres_output() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let keys = vec!["id".to_string()];
        let nonkeys = vec!["name".to_string(), "age".to_string()];

        let parts =
            build_upsert_sql("users", &columns, &keys, &nonkeys, DatabaseKind::Postgres).unwrap();

        assert_eq!(
            parts.prefix,
            "INSERT INTO users (\"id\", \"name\", \"age\")"
        );
        assert_eq!(
            parts.suffix,
            " ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"age\" = EXCLUDED.\"age\""
        );
    }

    #[test]
    fn upsert_sql_keeps_existing_mysql_output() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let keys = vec!["id".to_string()];
        let nonkeys = vec!["name".to_string(), "age".to_string()];

        let parts =
            build_upsert_sql("users", &columns, &keys, &nonkeys, DatabaseKind::Mysql).unwrap();

        assert_eq!(parts.prefix, "INSERT INTO users (id, name, age)");
        assert_eq!(
            parts.suffix,
            " ON DUPLICATE KEY UPDATE name = VALUES(name), age = VALUES(age)"
        );
    }

    #[test]
    fn update_sql_keeps_existing_dialect_output() {
        let nonkeys = vec!["name".to_string(), "age".to_string()];
        let keys = vec!["id".to_string()];

        assert_eq!(
            build_update_sql("users", &nonkeys, &keys, DatabaseKind::Postgres).unwrap(),
            "UPDATE users SET \"name\" = ?, \"age\" = ? WHERE \"id\" = ?"
        );
        assert_eq!(
            build_update_sql("users", &nonkeys, &keys, DatabaseKind::Mysql).unwrap(),
            "UPDATE users SET name = ?, age = ? WHERE id = ?"
        );
    }

    #[test]
    fn write_rows_builds_upsert_and_update_batches_through_chain() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ]];
        let keys = vec!["id".to_string()];

        let upsert = WriteRows::new(columns.clone(), rows.clone())
            .table("users")
            .key_columns(&keys)
            .mode(WriteMode::Upsert)
            .database_kind(DatabaseKind::Postgres)
            .build()
            .unwrap();
        assert_eq!(upsert.sql, "INSERT INTO users (\"id\", \"name\")");
        assert_eq!(
            upsert.upsert_suffix.as_deref(),
            Some(" ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"")
        );

        let update = WriteRows::new(columns, rows)
            .table("users")
            .key_columns(&keys)
            .mode(WriteMode::Update)
            .database_kind(DatabaseKind::Mysql)
            .build()
            .unwrap();
        assert_eq!(update.sql, "UPDATE users SET name = ? WHERE id = ?");
        assert!(update.upsert_suffix.is_none());
    }

    #[test]
    fn values_query_collects_postgres_placeholders_and_params() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![
                UnifiedValue::Int(1),
                UnifiedValue::String("Alice".to_string()),
            ],
            vec![
                UnifiedValue::Int(2),
                UnifiedValue::String("Bob".to_string()),
            ],
        ];

        let batch = WriteRows::new(columns, rows)
            .table("users")
            .mode(WriteMode::Insert)
            .database_kind(DatabaseKind::Postgres)
            .build()
            .unwrap();
        let query = batch
            .build_values_query(&batch.rows, DatabaseKind::Postgres)
            .unwrap();

        assert_eq!(
            query.sql,
            "INSERT INTO users (\"id\", \"name\") VALUES ($1, $2), ($3, $4)"
        );
        assert_eq!(
            query.params,
            vec![
                UnifiedValue::Int(1),
                UnifiedValue::String("Alice".to_string()),
                UnifiedValue::Int(2),
                UnifiedValue::String("Bob".to_string())
            ]
        );
    }

    #[test]
    fn values_query_collects_mysql_placeholders_and_upsert_suffix() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ]];
        let keys = vec!["id".to_string()];

        let batch = WriteRows::new(columns, rows)
            .table("users")
            .key_columns(&keys)
            .mode(WriteMode::Upsert)
            .database_kind(DatabaseKind::Mysql)
            .build()
            .unwrap();
        let query = batch
            .build_values_query(&batch.rows, DatabaseKind::Mysql)
            .unwrap();

        assert_eq!(
            query.sql,
            "INSERT INTO users (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)"
        );
        assert_eq!(
            query.params,
            vec![
                UnifiedValue::Int(1),
                UnifiedValue::String("Alice".to_string())
            ]
        );
    }

    #[test]
    fn update_query_reorders_values_by_sql_columns() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let row = vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
            UnifiedValue::Int(30),
        ];
        let keys = vec!["id".to_string()];

        let query =
            build_update_query("users", &columns, &row, &keys, DatabaseKind::Postgres).unwrap();

        assert_eq!(
            query.sql,
            "UPDATE users SET \"name\" = $1, \"age\" = $2 WHERE \"id\" = $3"
        );
        assert_eq!(
            query.params,
            vec![
                UnifiedValue::String("Alice".to_string()),
                UnifiedValue::Int(30),
                UnifiedValue::Int(1)
            ]
        );
    }

    #[test]
    fn delete_query_collects_key_values_only() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let row = vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ];
        let keys = vec!["id".to_string()];

        let query =
            build_delete_query("users", &columns, &row, &keys, DatabaseKind::Mysql).unwrap();

        assert_eq!(query.sql, "DELETE FROM users WHERE id = ?");
        assert_eq!(query.params, vec![UnifiedValue::Int(1)]);
    }

    #[test]
    fn dialect_dispatch_stays_at_wrapper_boundary() {
        let source = include_str!("sql_builder.rs");

        assert!(!source.contains(concat!("Write", "Dialect")));
        assert!(!source.contains(concat!("match self", ".kind")));
        assert!(!source.contains(concat!("match out", ".dialect")));
        assert!(!source.contains(concat!(".", "db_kind", "(")));
    }
}

/// 分离主键列和非主键列
pub fn split_keys_nonkeys(all_cols: &[String], key_cols: &[String]) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut nonkeys = Vec::new();
    for col in all_cols {
        if key_cols.contains(col) {
            keys.push(col.clone());
        } else {
            nonkeys.push(col.clone());
        }
    }
    (keys, nonkeys)
}

/// 构建 INSERT SQL（不含 VALUES，由 execute_batch 拼接）
pub fn build_insert_sql(table: &str, columns: &[String], kind: DatabaseKind) -> String {
    dispatch_backend!(kind, |DB| build_insert_sql_with_backend::<DB>(
        table, columns
    ))
}

fn build_insert_sql_with_backend<DB: SqlBackend>(table: &str, columns: &[String]) -> String {
    let mut ctx = BuildCtx::<DB>::new();
    InsertStatement { table, columns }.walk_ast(&mut ctx);
    ctx.finish()
}

/// Upsert SQL 拆分结果
pub struct UpsertParts {
    /// `INSERT INTO table (cols)`
    pub prefix: String,
    /// `ON DUPLICATE KEY UPDATE ...` 或 `ON CONFLICT (...) DO UPDATE SET ...`
    pub suffix: String,
}

/// 构建 upsert SQL，拆为 prefix + suffix
///
/// execute_batch 拼接顺序：prefix + VALUES (?,?,?) + suffix
pub fn build_upsert_sql(
    table: &str,
    columns: &[String],
    keys: &[String],
    nonkeys: &[String],
    kind: DatabaseKind,
) -> Result<UpsertParts> {
    dispatch_backend!(kind, |DB| build_upsert_sql_with_backend::<DB>(
        table, columns, keys, nonkeys
    ))
}

fn build_upsert_sql_with_backend<DB: SqlBackend>(
    table: &str,
    columns: &[String],
    keys: &[String],
    nonkeys: &[String],
) -> Result<UpsertParts>
where
    for<'a> ConflictFragment<'a, DB>: QueryFragment<DB>,
{
    let prefix = build_insert_sql_with_backend::<DB>(table, columns);

    if DB::REQUIRES_UPSERT_KEYS && keys.is_empty() {
        bail!("upsert 模式需要指定 key_columns");
    }

    let mut ctx = BuildCtx::<DB>::new();
    ConflictFragment::<DB> {
        keys,
        nonkeys,
        _backend: PhantomData,
    }
    .walk_ast(&mut ctx);
    let suffix = ctx.finish();

    Ok(UpsertParts { prefix, suffix })
}

/// 构建 UPDATE SQL（完整带占位符，走 execute_with_params 逐行执行）
pub fn build_update_sql(
    table: &str,
    nonkeys: &[String],
    keys: &[String],
    kind: DatabaseKind,
) -> Result<String> {
    dispatch_backend!(kind, |DB| build_update_sql_with_backend::<DB>(
        table, nonkeys, keys
    ))
}

fn build_update_sql_with_backend<DB: SqlBackend>(
    table: &str,
    nonkeys: &[String],
    keys: &[String],
) -> Result<String> {
    if keys.is_empty() {
        bail!("update 模式需要指定 key_columns");
    }

    let mut ctx = BuildCtx::<DB>::new();
    UpdateStatement {
        table,
        nonkeys,
        keys,
    }
    .walk_ast(&mut ctx);
    Ok(ctx.finish())
}

/// 构建 DELETE SQL（完整带占位符，走 execute_with_params 逐行执行）
pub fn build_delete_sql(table: &str, keys: &[String], kind: DatabaseKind) -> Result<String> {
    dispatch_backend!(kind, |DB| build_delete_sql_with_backend::<DB>(table, keys))
}

fn build_delete_sql_with_backend<DB: SqlBackend>(table: &str, keys: &[String]) -> Result<String> {
    if keys.is_empty() {
        bail!("delete 模式需要指定 key_columns");
    }

    let mut ctx = BuildCtx::<DB>::new();
    DeleteStatement { table, keys }.walk_ast(&mut ctx);
    Ok(ctx.finish())
}

/// 构建 INSERT/UPSERT 批量 VALUES SQL，并按行优先顺序收集绑定值。
pub fn build_values_query(
    base_sql: &str,
    rows: &[Vec<UnifiedValue>],
    suffix: &str,
    kind: DatabaseKind,
) -> Result<BuiltWriteQuery> {
    dispatch_backend!(kind, |DB| build_values_query_with_backend::<DB>(
        base_sql, rows, suffix
    ))
}

fn build_values_query_with_backend<DB: SqlBackend>(
    base_sql: &str,
    rows: &[Vec<UnifiedValue>],
    suffix: &str,
) -> Result<BuiltWriteQuery> {
    if rows.is_empty() {
        bail!("没有数据需要同步");
    }

    let mut ctx = BuildCtx::<DB>::new();
    BatchValuesStatement {
        base_sql,
        rows,
        suffix,
    }
    .walk_ast(&mut ctx);
    Ok(ctx.finish_query())
}

/// 构建 UPDATE SQL，并按 SET 列、WHERE key 列的顺序收集绑定值。
pub fn build_update_query(
    table: &str,
    columns: &[String],
    row: &[UnifiedValue],
    key_columns: &[String],
    kind: DatabaseKind,
) -> Result<BuiltWriteQuery> {
    dispatch_backend!(kind, |DB| build_update_query_with_backend::<DB>(
        table,
        columns,
        row,
        key_columns
    ))
}

fn build_update_query_with_backend<DB: SqlBackend>(
    table: &str,
    columns: &[String],
    row: &[UnifiedValue],
    key_columns: &[String],
) -> Result<BuiltWriteQuery> {
    if key_columns.is_empty() {
        bail!("update 模式需要指定 key_columns");
    }

    ensure_row_width_for_row(row, columns.len())?;
    let (keys, nonkeys) = split_keys_nonkeys(columns, key_columns);
    let nonkey_values = values_for_columns(columns, row, &nonkeys)?;
    let key_values = values_for_columns(columns, row, &keys)?;

    let mut ctx = BuildCtx::<DB>::new();
    UpdateValuesStatement {
        table,
        nonkeys: &nonkeys,
        nonkey_values: &nonkey_values,
        keys: &keys,
        key_values: &key_values,
    }
    .walk_ast(&mut ctx);
    Ok(ctx.finish_query())
}

/// 构建 DELETE SQL，并按 WHERE key 列的顺序收集绑定值。
pub fn build_delete_query(
    table: &str,
    columns: &[String],
    row: &[UnifiedValue],
    key_columns: &[String],
    kind: DatabaseKind,
) -> Result<BuiltWriteQuery> {
    dispatch_backend!(kind, |DB| build_delete_query_with_backend::<DB>(
        table,
        columns,
        row,
        key_columns
    ))
}

fn build_delete_query_with_backend<DB: SqlBackend>(
    table: &str,
    columns: &[String],
    row: &[UnifiedValue],
    key_columns: &[String],
) -> Result<BuiltWriteQuery> {
    if key_columns.is_empty() {
        bail!("delete 模式需要指定 key_columns");
    }

    ensure_row_width_for_row(row, columns.len())?;
    let (keys, _) = split_keys_nonkeys(columns, key_columns);
    let key_values = values_for_columns(columns, row, &keys)?;

    let mut ctx = BuildCtx::<DB>::new();
    DeleteValuesStatement {
        table,
        keys: &keys,
        key_values: &key_values,
    }
    .walk_ast(&mut ctx);
    Ok(ctx.finish_query())
}

fn ensure_row_width(rows: &[Vec<UnifiedValue>], expected: usize) -> Result<()> {
    for row in rows {
        ensure_row_width_for_row(row, expected)?;
    }
    Ok(())
}

fn ensure_row_width_for_row(row: &[UnifiedValue], expected: usize) -> Result<()> {
    if row.len() != expected {
        bail!(
            "写入数据列数不匹配：期望 {} 列，实际 {} 列",
            expected,
            row.len()
        );
    }
    Ok(())
}

fn values_for_columns(
    columns: &[String],
    row: &[UnifiedValue],
    selected_columns: &[String],
) -> Result<Vec<UnifiedValue>> {
    selected_columns
        .iter()
        .map(|selected| {
            columns
                .iter()
                .position(|column| column == selected)
                .and_then(|index| row.get(index))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("写入数据缺少列 '{}'", selected))
        })
        .collect()
}

/// 准备写入批次
pub fn prepare_write_batch(
    columns: &[String],
    rows_data: &[Vec<UnifiedValue>],
    table_name: &str,
    key_columns: &[String],
    mode: WriteMode,
    kind: DatabaseKind,
) -> Result<WriteBatch> {
    WriteRows::new(columns.to_vec(), rows_data.to_vec())
        .table(table_name)
        .key_columns(key_columns)
        .mode(mode)
        .database_kind(kind)
        .build()
}

/// 执行批量写入
///
/// SQL builder 负责生成占位符并整理绑定值，executor 只负责执行参数化 SQL。
pub async fn execute_db_write(
    batch: &WriteBatch,
    pool: &Arc<RdbmsPool>,
    batch_size: usize,
) -> Result<usize> {
    if batch.rows.is_empty() {
        return Ok(0);
    }

    let executor = pool.executor();
    let kind = database_kind_from_pool(pool.as_ref());
    let mut processed = 0usize;

    match batch.mode {
        WriteMode::Insert | WriteMode::Upsert => {
            for chunk in batch.rows.chunks(batch_size) {
                let query = batch.build_values_query(chunk, kind)?;
                executor
                    .execute_with_params(&query.sql, &query.params)
                    .await?;
                processed += chunk.len();
            }
        }
        WriteMode::Update | WriteMode::Delete => {
            for r in &batch.rows {
                let query = batch.build_row_query(r, kind)?;
                executor
                    .execute_with_params(&query.sql, &query.params)
                    .await?;
                processed += 1;
            }
        }
    }

    Ok(processed)
}

fn database_kind_from_pool(pool: &RdbmsPool) -> DatabaseKind {
    match pool {
        RdbmsPool::Postgres(_) => DatabaseKind::Postgres,
        RdbmsPool::Mysql(_) => DatabaseKind::Mysql,
    }
}

/// Upsert 前置校验：key_columns 对应的列在目标表上必须有 UNIQUE 索引（含 PRIMARY KEY）
///
/// MySQL ON DUPLICATE KEY UPDATE 和 PostgreSQL ON CONFLICT 都要求
/// 冲突列存在唯一约束，否则永远不会触发 UPDATE，导致数据重复。
pub async fn validate_upsert_keys(
    pool: &RdbmsPool,
    table: &str,
    key_columns: &[String],
) -> Result<()> {
    if key_columns.is_empty() {
        bail!("upsert 模式需要指定 key_columns");
    }

    let unique_columns = query_unique_columns(pool, table).await?;
    if unique_columns.is_empty() {
        bail!(
            "目标表 '{}' 没有任何唯一索引或主键，无法执行 upsert。\n\
             请为 key_columns {:?} 中的列创建 UNIQUE 索引，例如：\n\
             ALTER TABLE {} ADD UNIQUE INDEX idx_upsert ({});",
            table,
            key_columns,
            table,
            key_columns.join(", ")
        );
    }

    for kc in key_columns {
        if !unique_columns.contains(kc) {
            bail!(
                "目标表 '{}' 的 key_column '{}' 没有唯一索引。\n\
                 当前的唯一索引列: {:?}\n\
                 请为该列创建 UNIQUE 索引，例如：\n\
                 ALTER TABLE {} ADD UNIQUE INDEX idx_{} ({});",
                table,
                kc,
                unique_columns,
                table,
                kc,
                kc
            );
        }
    }

    Ok(())
}

/// 查询目标表上所有 UNIQUE 约束覆盖的列（含 PRIMARY KEY）
async fn query_unique_columns(pool: &RdbmsPool, table: &str) -> Result<Vec<String>> {
    match pool {
        RdbmsPool::Mysql(p) => {
            let sql = format!(
                "SELECT DISTINCT kcu.COLUMN_NAME \
                 FROM information_schema.TABLE_CONSTRAINTS tc \
                 JOIN information_schema.KEY_COLUMN_USAGE kcu \
                     ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                     AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA \
                     AND tc.TABLE_NAME = kcu.TABLE_NAME \
                 WHERE tc.TABLE_SCHEMA = DATABASE() \
                     AND tc.TABLE_NAME = '{}' \
                     AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')",
                table
            );
            let rows = sqlx::query(&sql).fetch_all(p).await?;
            use sqlx::Row;
            Ok(rows
                .iter()
                .filter_map(|r| r.try_get("COLUMN_NAME").ok())
                .collect())
        }
        RdbmsPool::Postgres(p) => {
            let sql = format!(
                "SELECT DISTINCT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                     ON tc.constraint_name = kcu.constraint_name \
                     AND tc.table_schema = kcu.table_schema \
                 WHERE tc.table_schema = 'public' \
                     AND tc.table_name = '{}' \
                     AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')",
                table
            );
            let rows = sqlx::query(&sql).fetch_all(p).await?;
            use sqlx::Row;
            Ok(rows
                .iter()
                .filter_map(|r| r.try_get("column_name").ok())
                .collect())
        }
    }
}
