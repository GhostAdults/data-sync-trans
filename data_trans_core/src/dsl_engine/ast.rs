/// AST 节点定义
/// 将 AST 定义独立出来，便于扩展

use serde_json::Value;

/// 比较操作符
#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,  // ==
    Ne,  // !=
    Gt,  // >
    Lt,  // <
    Ge,  // >=
    Le,  // <=
}

/// 表达式节点
#[derive(Debug, Clone)]
pub enum Expr {
    /// 字段引用：source.field_name
    Field(String),

    /// 字符串字面量：'text'
    StringLiteral(String),

    /// 数字字面量：123
    NumberLiteral(i64),

    /// 函数调用：func_name(arg1, arg2, ...)
    /// 使用字符串名称而不是枚举，支持动态注册
    FuncCall {
        name: String,
        args: Vec<Expr>
    },

    /// 比较表达式：left op right
    Compare {
        left: Box<Expr>,
        op: CompareOp,
        right: Box<Expr>
    },
}

/// 节点求值上下文
pub struct EvalContext<'a> {
    /// 源数据
    pub source: &'a Value,
}

impl<'a> EvalContext<'a> {
    pub fn new(source: &'a Value) -> Self {
        Self { source }
    }
}
