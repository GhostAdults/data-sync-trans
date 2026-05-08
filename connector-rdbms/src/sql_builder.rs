//! RDBMS SQL 构建与写入执行。
//!
//! 本模块负责把 Relus 的统一数据模型转换成具体数据库可执行的
//! 参数化 SQL，并通过 `DatabaseExecutor` 执行写入。
//!
//! 设计边界：
//! - SQL AST 层使用 `SqlBuilder<DB>` / `WriteSqlBuilder<DB>`，
//!   通过 `DB: SqlBackend` 做编译期后端分派。
//! - PostgreSQL/MySQL 的差异集中在 `PostgresBackend`、`MysqlBackend`
//!   和对应的 `QueryFragment<DB>` 实现里。
//! - 运行时数据库类型只在 `RdbmsSqlBuilder` 或 `execute_rdbms_write`
//!   这种 RDBMS facade 边界 match 一次。
//! - writer 层只需要传入 `WriteRows`、目标表、写入模式和 key_columns，
//!   不需要了解 SQL 占位符、identifier 引号或 upsert 方言差异。
//!
//! 读侧主要入口：
//! - `SqlBuilder<DB>`：已知后端类型时使用。
//! - `RdbmsSqlBuilder`：只有 `DatabaseKind` / `RdbmsPool` 时使用。
//!
//! 写侧主要入口：
//! - `WriteRows`：列名和多行值的输入模型。
//! - `execute_rdbms_write`：writer 调用的统一执行入口。

use anyhow::{bail, Result};
use std::marker::PhantomData;
use std::sync::Arc;

use relus_common::job_config::WriteMode;
use relus_common::types::UnifiedValue;

use super::pool::{DatabaseKind, RdbmsPool};

#[derive(Debug, Clone, Copy)]
pub struct PostgresBackend;

#[derive(Debug, Clone, Copy)]
pub struct MysqlBackend;

/// SQL 方言能力接口。
///
/// 每个数据库后端通过实现该 trait 定义 identifier 引用规则、
/// select 列表达式处理方式、占位符格式和 upsert key 约束。
pub trait SqlBackend {
    /// 当前后端执行 upsert 时是否必须显式指定冲突列。
    const REQUIRES_UPSERT_KEYS: bool;

    /// 将逻辑 identifier 转成当前后端可用的 SQL identifier。
    fn identifier(identifier: &str) -> String;

    /// 将用户传入的列列表转换成当前后端的 select 列表达式。
    fn column_list(columns: &str) -> String;

    /// 将单个 identifier 追加到 SQL buffer。
    fn push_identifier(out: &mut String, ident: &str);

    /// 将当前后端的参数占位符追加到 SQL buffer。
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

/// 读侧 SQL 构建器。
///
/// `DB` 是具体数据库后端，例如 `PostgresBackend` 或 `MysqlBackend`。
/// 当调用方已经知道后端类型时，优先使用该泛型 builder，避免在
/// SQL AST 层做运行时分支。
#[derive(Debug, Clone, Copy)]
pub struct SqlBuilder<DB> {
    _backend: PhantomData<DB>,
}

/// RDBMS 读侧 SQL facade。
///
/// 当调用方只有 `DatabaseKind` 或 `RdbmsPool` 时使用该 enum。
/// 它只在边界处 match 一次，然后委托给具体的 `SqlBuilder<DB>`。
#[derive(Debug, Clone, Copy)]
pub enum RdbmsSqlBuilder {
    /// PostgreSQL 读侧 SQL builder。
    Postgres(SqlBuilder<PostgresBackend>),
    /// MySQL 读侧 SQL builder。
    Mysql(SqlBuilder<MysqlBackend>),
}

/// 主键范围查询参数。
///
/// 用于 reader 按主键范围拆分任务时生成形如
/// `pk >= min AND pk < max` 或 `pk >= min AND pk <= max` 的查询。
#[derive(Debug, Clone, Copy)]
pub struct PkRangeSelect<'a> {
    /// 查询表名。
    pub table: &'a str,
    /// 查询列列表，通常来自 reader 配置。
    pub columns: &'a str,
    /// 额外 where 条件，会和主键范围条件用 AND 拼接。
    pub where_clause: Option<&'a str>,
    /// 用于分片的主键列。
    pub pk: &'a str,
    /// 范围起始值。
    pub min: &'a str,
    /// 范围结束值。
    pub max: &'a str,
    /// 是否包含结束值。
    pub inclusive_end: bool,
}

impl<DB> Default for SqlBuilder<DB> {
    fn default() -> Self {
        Self {
            _backend: PhantomData,
        }
    }
}

impl<DB: SqlBackend> SqlBuilder<DB> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn select(&self, columns: &str, table: &str) -> SelectBuilder {
        SelectSqlBuilder::<DB>::select(columns, table)
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
        SelectSqlBuilder::<DB>::pk_min_max(table, pk, where_clause)
    }

    pub fn pk_range_select(&self, range: PkRangeSelect<'_>) -> String {
        SelectSqlBuilder::<DB>::pk_range_select(range)
    }

    pub fn null_pk_select(
        &self,
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        pk: &str,
    ) -> String {
        SelectSqlBuilder::<DB>::null_pk_select(table, columns, where_clause, pk)
    }

    pub fn limit_offset_select(
        &self,
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> String {
        SelectSqlBuilder::<DB>::limit_offset_select(table, columns, where_clause, limit, offset)
    }

    pub fn literal_string(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub fn column_list(&self, columns: &str) -> String {
        DB::column_list(columns)
    }
}

impl RdbmsSqlBuilder {
    pub fn new(kind: DatabaseKind) -> Self {
        match kind {
            DatabaseKind::Postgres => Self::Postgres(SqlBuilder::<PostgresBackend>::new()),
            DatabaseKind::Mysql => Self::Mysql(SqlBuilder::<MysqlBackend>::new()),
        }
    }

    pub fn from_pool(pool: &RdbmsPool) -> Self {
        match pool {
            RdbmsPool::Postgres(_) => Self::Postgres(SqlBuilder::<PostgresBackend>::new()),
            RdbmsPool::Mysql(_) => Self::Mysql(SqlBuilder::<MysqlBackend>::new()),
        }
    }

    pub fn select(&self, columns: &str, table: &str) -> SelectBuilder {
        match self {
            Self::Postgres(builder) => builder.select(columns, table),
            Self::Mysql(builder) => builder.select(columns, table),
        }
    }

    pub fn custom_query(&self, query: &str) -> SelectBuilder {
        match self {
            Self::Postgres(builder) => builder.custom_query(query),
            Self::Mysql(builder) => builder.custom_query(query),
        }
    }

    pub fn count(&self, table: &str, custom_query: Option<&str>) -> String {
        match self {
            Self::Postgres(builder) => builder.count(table, custom_query),
            Self::Mysql(builder) => builder.count(table, custom_query),
        }
    }

    pub fn pk_min_max(&self, table: &str, pk: &str, where_clause: Option<&str>) -> String {
        match self {
            Self::Postgres(builder) => builder.pk_min_max(table, pk, where_clause),
            Self::Mysql(builder) => builder.pk_min_max(table, pk, where_clause),
        }
    }

    pub fn pk_range_select(&self, range: PkRangeSelect<'_>) -> String {
        match self {
            Self::Postgres(builder) => builder.pk_range_select(range),
            Self::Mysql(builder) => builder.pk_range_select(range),
        }
    }

    pub fn null_pk_select(
        &self,
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        pk: &str,
    ) -> String {
        match self {
            Self::Postgres(builder) => builder.null_pk_select(table, columns, where_clause, pk),
            Self::Mysql(builder) => builder.null_pk_select(table, columns, where_clause, pk),
        }
    }

    pub fn limit_offset_select(
        &self,
        table: &str,
        columns: &str,
        where_clause: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> String {
        match self {
            Self::Postgres(builder) => {
                builder.limit_offset_select(table, columns, where_clause, limit, offset)
            }
            Self::Mysql(builder) => {
                builder.limit_offset_select(table, columns, where_clause, limit, offset)
            }
        }
    }

    pub fn column_list(&self, columns: &str) -> String {
        match self {
            Self::Postgres(builder) => builder.column_list(columns),
            Self::Mysql(builder) => builder.column_list(columns),
        }
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
        let min = SqlBuilder::<DB>::literal_string(range.min);
        let max = SqlBuilder::<DB>::literal_string(range.max);
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

/// 可链式追加条件和分页的 SELECT SQL 构建结果。
///
/// `SqlBuilder<DB>::select` 和 `custom_query` 返回该类型，
/// 调用方可以继续追加 where、limit/offset，最后通过 `build` 得到 SQL 字符串。
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

/// SQL builder 生成的参数化 SQL 和绑定值。
#[derive(Debug, Clone, PartialEq)]
pub struct BuiltWriteQuery {
    /// 参数化 SQL，例如 PostgreSQL 使用 `$1`，MySQL 使用 `?`。
    pub sql: String,
    /// 按 SQL 占位符出现顺序展开后的参数值。
    pub params: Vec<UnifiedValue>,
}

/// RDBMS 写入行模型。
///
/// `columns` 表示目标表写入列顺序；`values` 中每个 `Vec<UnifiedValue>` 都是一行数据，
/// 且每个 value 的位置必须和 `columns` 中的列位置一一对应。
/// 例如 `columns = ["id", "name"]` 时，`values[0] = [1, "Alice"]`
/// 表示 `id = 1, name = "Alice"`。
pub struct WriteRows {
    /// 目标表列名，决定 SQL 字段顺序和每行 value 的解释方式。
    pub columns: Vec<String>,
    /// 多行写入值；内层 value 按 `columns` 的顺序排列。
    pub values: Vec<Vec<UnifiedValue>>,
}

impl WriteRows {
    pub fn new(columns: Vec<String>, values: Vec<Vec<UnifiedValue>>) -> Self {
        Self { columns, values }
    }
}

/// Stateless write-side SQL builder using compile-time backend dispatch.
struct WriteSqlBuilder<DB> {
    _backend: PhantomData<DB>,
}

impl<DB> Default for WriteSqlBuilder<DB> {
    fn default() -> Self {
        Self {
            _backend: PhantomData,
        }
    }
}

impl<DB: SqlBackend> WriteSqlBuilder<DB>
where
    for<'a> ConflictFragment<'a, DB>: QueryFragment<DB>,
{
    pub fn build_many(
        table: &str,
        mode: WriteMode,
        key_columns: &[String],
        columns: &[String],
        rows: &[Vec<UnifiedValue>],
    ) -> Result<BuiltWriteQuery> {
        ensure_row_width(rows, columns.len())?;
        match mode {
            WriteMode::Insert => build_values_query_with_backend::<DB>(
                &build_insert_sql_with_backend::<DB>(table, columns),
                rows,
                "",
            ),
            WriteMode::Upsert => {
                let (keys, nonkeys) = split_keys_nonkeys(columns, key_columns);
                let parts = build_upsert_sql_with_backend::<DB>(table, columns, &keys, &nonkeys)?;
                build_values_query_with_backend::<DB>(&parts.prefix, rows, &parts.suffix)
            }
            WriteMode::Update | WriteMode::Delete => {
                bail!("{} 模式不支持批量 VALUES SQL", mode.as_str())
            }
        }
    }

    pub fn build_one(
        table: &str,
        mode: WriteMode,
        key_columns: &[String],
        columns: &[String],
        row: &[UnifiedValue],
    ) -> Result<BuiltWriteQuery> {
        match mode {
            WriteMode::Insert | WriteMode::Upsert => {
                Self::build_many(table, mode, key_columns, columns, &[row.to_vec()])
            }
            WriteMode::Update => {
                build_update_query_with_backend::<DB>(table, columns, row, key_columns)
            }
            WriteMode::Delete => {
                build_delete_query_with_backend::<DB>(table, columns, row, key_columns)
            }
        }
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

/// 根据连接池类型生成并执行 RDBMS 写入 SQL。
///
/// 用法：
/// 1. writer 层先把 `MappingRow` 批次转换成 `WriteRows`。
/// 2. 调用本函数并传入目标表、写入模式、key_columns 和 batch_size。
/// 3. 本函数根据 `RdbmsPool` 选择 PostgreSQL/MySQL 后端，生成参数化 SQL 并执行。
///
/// 参数说明：
/// - `pool`: 目标库连接池，也是运行时选择数据库后端的唯一边界。
/// - `table`: 目标表名。
/// - `mode`: 写入模式，支持 insert/upsert/update/delete。
/// - `key_columns`: upsert/update/delete 的匹配列；insert 可为空。
/// - `write_rows`: 待写入数据，`write_rows.columns` 是列顺序，
///   `write_rows.values[*]` 是对应 value 列表。
/// - `batch_size`: insert/upsert 每批拼接的行数；传 0 时按 1 处理。
///
/// 执行过程：
/// - 空数据直接返回 `Ok(0)`。
/// - 在 `RdbmsPool` 边界 match 一次，之后进入具体后端的编译期分派。
/// - insert/upsert 按 `batch_size` 分片，批量生成一条 VALUES SQL。
/// - update/delete 每行生成一条带参数的 SQL。
/// - executor 只负责执行 SQL builder 返回的 `sql + params`。
pub async fn execute_rdbms_write(
    pool: &Arc<RdbmsPool>,
    table: &str,
    mode: WriteMode,
    key_columns: &[String],
    write_rows: WriteRows,
    batch_size: usize,
) -> Result<usize> {
    if write_rows.values.is_empty() {
        return Ok(0);
    }

    let executor = pool.executor();
    // 运行时数据库类型只在连接池边界判断一次，SQL 构建进入具体后端泛型实现。
    match pool.as_ref() {
        RdbmsPool::Postgres(_) => {
            execute_write_with_backend::<PostgresBackend>(
                executor.as_ref(),
                table,
                mode,
                key_columns,
                &write_rows,
                batch_size,
            )
            .await
        }
        RdbmsPool::Mysql(_) => {
            execute_write_with_backend::<MysqlBackend>(
                executor.as_ref(),
                table,
                mode,
                key_columns,
                &write_rows,
                batch_size,
            )
            .await
        }
    }
}

async fn execute_write_with_backend<DB: SqlBackend>(
    executor: &dyn super::pool::DatabaseExecutor,
    table: &str,
    mode: WriteMode,
    key_columns: &[String],
    write_rows: &WriteRows,
    batch_size: usize,
) -> Result<usize>
where
    for<'a> ConflictFragment<'a, DB>: QueryFragment<DB>,
{
    let mut processed = 0usize;
    let batch_size = batch_size.max(1);
    match mode {
        WriteMode::Insert | WriteMode::Upsert => {
            // insert/upsert 可以把多行 values 合并成一条参数化 SQL。
            for chunk in write_rows.values.chunks(batch_size) {
                let query = WriteSqlBuilder::<DB>::build_many(
                    table,
                    mode,
                    key_columns,
                    &write_rows.columns,
                    chunk,
                )?;
                executor
                    .execute_with_params(&query.sql, &query.params)
                    .await?;
                processed += chunk.len();
            }
        }
        WriteMode::Update | WriteMode::Delete => {
            // update/delete 的 WHERE 条件依赖当前行 key 值，逐行生成并执行。
            for row in &write_rows.values {
                let query = WriteSqlBuilder::<DB>::build_one(
                    table,
                    mode,
                    key_columns,
                    &write_rows.columns,
                    row,
                )?;
                executor
                    .execute_with_params(&query.sql, &query.params)
                    .await?;
                processed += 1;
            }
        }
    }
    Ok(processed)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_quotes_postgres_columns_only() {
        let sql = SqlBuilder::<PostgresBackend>::new()
            .select("id, name", "users")
            .build();
        println!("Generated SQL: {}", sql);
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM users");
    }

    #[test]
    fn select_keeps_mysql_columns() {
        let sql = SqlBuilder::<MysqlBackend>::new()
            .select("id, name", "users")
            .build();

        assert_eq!(sql, "SELECT id, name FROM users");
    }

    #[test]
    fn count_supports_table_and_custom_query() {
        let builder = SqlBuilder::<MysqlBackend>::new();

        assert_eq!(builder.count("users", None), "SELECT COUNT(*) FROM users");
        assert_eq!(
            builder.count("users", Some("SELECT id FROM users WHERE active = 1")),
            "SELECT COUNT(*) FROM (SELECT id FROM users WHERE active = 1) AS subquery"
        );
    }

    #[test]
    fn range_query_combines_where_clause() {
        let sql = SqlBuilder::<PostgresBackend>::new().pk_range_select(PkRangeSelect {
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
        let sql = SqlBuilder::<MysqlBackend>::new().null_pk_select(
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
        let sql = SqlBuilder::<MysqlBackend>::new().limit_offset_select(
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
        assert_eq!(
            SqlBuilder::<MysqlBackend>::literal_string("O'Reilly"),
            "'O''Reilly'"
        );
    }

    #[test]
    fn write_rows_only_keeps_columns_and_rows() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ]];

        let write_rows = WriteRows::new(columns.clone(), rows.clone());

        assert_eq!(write_rows.columns, columns);
        assert_eq!(write_rows.values, rows);
    }

    #[test]
    fn insert_sql_keeps_existing_dialect_output() {
        let columns = vec!["id".to_string(), "name".to_string()];

        assert_eq!(
            build_insert_sql_with_backend::<PostgresBackend>("users", &columns),
            "INSERT INTO users (\"id\", \"name\")"
        );
        assert_eq!(
            build_insert_sql_with_backend::<MysqlBackend>("users", &columns),
            "INSERT INTO users (id, name)"
        );
    }

    #[test]
    fn upsert_sql_keeps_existing_postgres_output() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let keys = vec!["id".to_string()];
        let nonkeys: Vec<String> = vec!["name".to_string(), "age".to_string()];

        let parts =
            build_upsert_sql_with_backend::<PostgresBackend>("users", &columns, &keys, &nonkeys)
                .unwrap();

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
            build_upsert_sql_with_backend::<MysqlBackend>("users", &columns, &keys, &nonkeys)
                .unwrap();

        assert_eq!(parts.prefix, "INSERT INTO users (id, name, age)");
        assert_eq!(
            parts.suffix,
            " ON DUPLICATE KEY UPDATE name = VALUES(name), age = VALUES(age)"
        );
    }

    #[test]
    fn write_sql_builder_builds_upsert_and_update_queries() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ]];
        let keys = vec!["id".to_string()];

        let upsert = WriteSqlBuilder::<PostgresBackend>::build_many(
            "users",
            WriteMode::Upsert,
            &keys,
            &columns,
            &rows,
        )
        .unwrap();
        assert_eq!(
            upsert.sql,
            "INSERT INTO users (\"id\", \"name\") VALUES ($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
        );

        let update = WriteSqlBuilder::<MysqlBackend>::build_one(
            "users",
            WriteMode::Update,
            &keys,
            &columns,
            &rows[0],
        )
        .unwrap();
        assert_eq!(update.sql, "UPDATE users SET name = ? WHERE id = ?");
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

        let query = WriteSqlBuilder::<PostgresBackend>::build_many(
            "users",
            WriteMode::Insert,
            &[],
            &columns,
            &rows,
        )
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

        let query = WriteSqlBuilder::<MysqlBackend>::build_many(
            "users",
            WriteMode::Upsert,
            &keys,
            &columns,
            &rows,
        )
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
    fn postgres_upsert_requires_key_columns() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![
            UnifiedValue::Int(1),
            UnifiedValue::String("Alice".to_string()),
        ]];

        let err = WriteSqlBuilder::<PostgresBackend>::build_many(
            "users",
            WriteMode::Upsert,
            &[],
            &columns,
            &rows,
        )
        .unwrap_err();

        assert!(err.to_string().contains("upsert 模式需要指定 key_columns"));
    }

    #[test]
    fn write_sql_builder_rejects_mismatched_row_width() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec![UnifiedValue::Int(1)]];

        let err = WriteSqlBuilder::<MysqlBackend>::build_many(
            "users",
            WriteMode::Insert,
            &[],
            &columns,
            &rows,
        )
        .unwrap_err();

        assert!(err.to_string().contains("写入数据列数不匹配"));
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

        let query = WriteSqlBuilder::<PostgresBackend>::build_one(
            "users",
            WriteMode::Update,
            &keys,
            &columns,
            &row,
        )
        .unwrap();

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

        let query = WriteSqlBuilder::<MysqlBackend>::build_one(
            "users",
            WriteMode::Delete,
            &keys,
            &columns,
            &row,
        )
        .unwrap();

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
