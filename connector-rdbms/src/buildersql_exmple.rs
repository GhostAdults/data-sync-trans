use std::fmt;

// ========= 基础数据类型 (已有 UnifiedValue)=========

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Text(String),
    Null,
    Bool(bool),
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}

// ========= SQL 输出上下文（类似 AstPass 思路） =========

#[derive(Debug, Default)]
pub struct BuildCtx {
    sql: String,
    binds: Vec<Value>,
    idx: usize,
}

impl BuildCtx {
    fn push_sql(&mut self, s: &str) {
        self.sql.push_str(s);
    }

    fn push_ident(&mut self, ident: &str) {
        // 这里只做最简示例：双引号包裹，转义 "
        self.sql.push('"');
        self.sql.push_str(&ident.replace('"', "\"\""));
        self.sql.push('"');
    }

    fn push_bind(&mut self, v: Value) {
        self.idx += 1;
        self.sql.push('$');
        self.sql.push_str(&self.idx.to_string());
        self.binds.push(v);
    }

    fn finish(self) -> BuiltQuery {
        BuiltQuery {
            sql: self.sql,
            binds: self.binds,
        }
    }
}

#[derive(Debug)]
pub struct BuiltQuery {
    pub sql: String,
    pub binds: Vec<Value>,
}

impl fmt::Display for BuiltQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "SQL: {}", self.sql)?;
        writeln!(f, "BINDS: {:?}", self.binds)
    }
}

// ========= 可构建 AST 节点 =========

trait QueryFragment {
    fn walk_ast(&self, out: &mut BuildCtx);
}

// ========= INSERT Builder =========

pub fn insert_into(table: impl Into<String>) -> IncompleteInsertStatement {
    IncompleteInsertStatement {
        table: table.into(),
        operator: InsertOperator::Insert,
    }
}

#[derive(Debug, Clone, Copy)]
pub enum InsertOperator {
    Insert,
    Replace, // 可选扩展
}

impl QueryFragment for InsertOperator {
    fn walk_ast(&self, out: &mut BuildCtx) {
        match self {
            InsertOperator::Insert => out.push_sql("INSERT"),
            InsertOperator::Replace => out.push_sql("REPLACE"),
        }
    }
}

// “未完成插入语句”：只有目标表，没有 values/default_values
pub struct IncompleteInsertStatement {
    table: String,
    operator: InsertOperator,
}

impl IncompleteInsertStatement {
    pub fn values(self, rows: Vec<RowValues>) -> InsertStatement {
        InsertStatement::new(self.table, self.operator, InsertRecords::Rows(rows))
    }

    pub fn default_values(self) -> InsertStatement {
        InsertStatement::new(self.table, self.operator, InsertRecords::DefaultValues)
    }
}

// 一行数据：[(col, value), ...]
pub type RowValues = Vec<(String, Value)>;

#[derive(Debug, Clone)]
pub enum InsertRecords {
    DefaultValues,
    Rows(Vec<RowValues>),
}

#[derive(Debug, Clone)]
pub enum OnConflict {
    DoNothing,
    DoUpdate {
        target_cols: Vec<String>,
        assignments: Vec<(String, ValueExpr)>,
    },
}

#[derive(Debug, Clone)]
pub enum ValueExpr {
    Bind(Value),      // 例如: name = $1
    Excluded(String), // 例如: name = EXCLUDED.name
    Raw(String),      // 例如: count = count + 1
}

#[derive(Debug, Clone, Default)]
pub struct ReturningClause {
    cols: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    table: String,
    operator: InsertOperator,
    records: InsertRecords,
    on_conflict: Option<OnConflict>,
    returning: Option<ReturningClause>,
}

impl InsertStatement {
    fn new(table: String, operator: InsertOperator, records: InsertRecords) -> Self {
        Self {
            table,
            operator,
            records,
            on_conflict: None,
            returning: None,
        }
    }

    pub fn on_conflict_do_nothing(mut self) -> Self {
        self.on_conflict = Some(OnConflict::DoNothing);
        self
    }

    pub fn on_conflict_do_update(
        mut self,
        target_cols: Vec<&str>,
        assignments: Vec<(&str, ValueExpr)>,
    ) -> Self {
        self.on_conflict = Some(OnConflict::DoUpdate {
            target_cols: target_cols.into_iter().map(|s| s.to_string()).collect(),
            assignments: assignments
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        });
        self
    }

    pub fn returning(mut self, cols: Vec<&str>) -> Self {
        self.returning = Some(ReturningClause {
            cols: cols.into_iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    pub fn build(&self) -> BuiltQuery {
        let mut ctx = BuildCtx::default();
        self.walk_ast(&mut ctx);
        ctx.finish()
    }
}

impl QueryFragment for InsertStatement {
    fn walk_ast(&self, out: &mut BuildCtx) {
        // INSERT INTO "table"
        self.operator.walk_ast(out);
        out.push_sql(" INTO ");
        out.push_ident(&self.table);
        out.push_sql(" ");

        // records
        match &self.records {
            InsertRecords::DefaultValues => {
                out.push_sql("DEFAULT VALUES");
            }
            InsertRecords::Rows(rows) => {
                if rows.is_empty() {
                    // 简化策略：空 rows 退化成不会插入的语句
                    out.push_sql("SELECT 1 WHERE 1=0");
                    return;
                }

                // 假设所有行列集合一致（简单实现）
                let cols: Vec<String> = rows[0].iter().map(|(c, _)| c.clone()).collect();

                out.push_sql("(");
                for (i, c) in cols.iter().enumerate() {
                    if i > 0 {
                        out.push_sql(", ");
                    }
                    out.push_ident(c);
                }
                out.push_sql(") VALUES ");

                for (r_idx, row) in rows.iter().enumerate() {
                    if r_idx > 0 {
                        out.push_sql(", ");
                    }
                    out.push_sql("(");

                    // 按 cols 顺序取值
                    for (c_idx, col) in cols.iter().enumerate() {
                        if c_idx > 0 {
                            out.push_sql(", ");
                        }
                        let val = row
                            .iter()
                            .find(|(c, _)| c == col)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(Value::Null);
                        out.push_bind(val);
                    }
                    out.push_sql(")");
                }
            }
        }

        // ON CONFLICT
        if let Some(conflict) = &self.on_conflict {
            match conflict {
                OnConflict::DoNothing => {
                    out.push_sql(" ON CONFLICT DO NOTHING");
                }
                OnConflict::DoUpdate {
                    target_cols,
                    assignments,
                } => {
                    out.push_sql(" ON CONFLICT (");
                    for (i, c) in target_cols.iter().enumerate() {
                        if i > 0 {
                            out.push_sql(", ");
                        }
                        out.push_ident(c);
                    }
                    out.push_sql(") DO UPDATE SET ");

                    for (i, (col, expr)) in assignments.iter().enumerate() {
                        if i > 0 {
                            out.push_sql(", ");
                        }
                        out.push_ident(col);
                        out.push_sql(" = ");

                        match expr {
                            ValueExpr::Bind(v) => out.push_bind(v.clone()),
                            ValueExpr::Excluded(c) => {
                                out.push_sql("EXCLUDED.");
                                out.push_ident(c);
                            }
                            ValueExpr::Raw(s) => out.push_sql(s),
                        }
                    }
                }
            }
        }

        // RETURNING
        if let Some(ret) = &self.returning {
            if !ret.cols.is_empty() {
                out.push_sql(" RETURNING ");
                for (i, c) in ret.cols.iter().enumerate() {
                    if i > 0 {
                        out.push_sql(", ");
                    }
                    out.push_ident(c);
                }
            }
        }
    }
}

// ========= demo =========

fn main() {
    // 1) 单条
    let q1 = insert_into("users")
        .values(vec![vec![
            ("name".into(), "Alice".into()),
            ("age".into(), Some(18).into()),
        ]])
        .returning(vec!["id", "name"])
        .build();
    println!("{}", q1);

    // 2) 批量
    let q2 = insert_into("users")
        .values(vec![
            vec![
                ("name".into(), "Bob".into()),
                ("age".into(), Some(20).into()),
            ],
            vec![
                ("name".into(), "Cindy".into()),
                ("age".into(), None::<i32>.into()),
            ],
        ])
        .on_conflict_do_nothing()
        .build();
    println!("{}", q2);

    // 3) upsert
    let q3 = insert_into("users")
        .values(vec![vec![
            ("name".into(), "Alice".into()),
            ("age".into(), 30.into()),
        ]])
        .on_conflict_do_update(
            vec!["name"],
            vec![
                ("age", ValueExpr::Excluded("age".into())),
                ("updated_flag", ValueExpr::Bind(true.into())),
            ],
        )
        .returning(vec!["id", "age"])
        .build();
    println!("{}", q3);

    // 4) default values
    let q4 = insert_into("audit_log")
        .default_values()
        .returning(vec!["id"])
        .build();
    println!("{}", q4);
}
