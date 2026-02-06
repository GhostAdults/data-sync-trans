use serde_json::Value;
use std::collections::HashMap;
use super::ast::{Expr, CompareOp, EvalContext};
use super::registry::FunctionRegistry;

/// 求值器：负责遍历 AST 并执行节点
pub struct Evaluator {
    /// 函数注册表
    registry: FunctionRegistry,
}

impl Evaluator {
    pub fn new() -> Self {
        Self {
            registry: FunctionRegistry::new(),
        }
    }

    /// 获取函数注册表的可变引用（用于注册自定义函数）
    pub fn registry_mut(&mut self) -> &mut FunctionRegistry {
        &mut self.registry
    }

    /// 求值表达式 - 统一入口，委托给具体的节点处理函数
    pub fn eval(&self, expr: &Expr, ctx: &EvalContext) -> Value {
        match expr {
            Expr::Field(_) => eval_field(expr, ctx),
            Expr::StringLiteral(_) => eval_literal(expr),
            Expr::NumberLiteral(_) => eval_literal(expr),
            Expr::FuncCall { .. } => eval_call(expr, ctx, &self.registry),
            Expr::Compare { .. } => eval_compare(expr, ctx, &self.registry),
        }
    }
}

impl Default for Evaluator {
    fn default() -> Self {
        Self::new()
    }
}

// ==========================================
// 独立的节点求值函数
// ==========================================

/// 求值字面量节点（字符串、数字）
fn eval_literal(expr: &Expr) -> Value {
    match expr {
        Expr::StringLiteral(s) => Value::String(s.to_string()),
        Expr::NumberLiteral(n) => Value::Number((*n).into()),
        _ => Value::Null,
    }
}

/// 求值字段引用节点
fn eval_field(expr: &Expr, ctx: &EvalContext) -> Value {
    match expr {
        Expr::Field(key) => ctx.source.get(key).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// 求值函数调用节点
fn eval_call(expr: &Expr, ctx: &EvalContext, registry: &FunctionRegistry) -> Value {
    match expr {
        Expr::FuncCall { name, args } => {
            // 创建求值闭包 - 递归调用 eval_expr
            let eval_fn = |arg_expr: &Expr| eval_expr(arg_expr, ctx, registry);

            // 通过注册表调用函数
            match registry.call(name, args, &eval_fn) {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("函数调用失败: {}", e);
                    Value::Null
                }
            }
        },
        _ => Value::Null,
    }
}

/// 求值比较表达式节点
fn eval_compare(expr: &Expr, ctx: &EvalContext, registry: &FunctionRegistry) -> Value {
    match expr {
        Expr::Compare { left, op, right } => {
            let left_val = eval_expr(left, ctx, registry);
            let right_val = eval_expr(right, ctx, registry);

            let result = match op {
                CompareOp::Eq => left_val == right_val,
                CompareOp::Ne => left_val != right_val,
                CompareOp::Gt => {
                    match (left_val.as_i64(), right_val.as_i64()) {
                        (Some(l), Some(r)) => l > r,
                        _ => false,
                    }
                },
                CompareOp::Lt => {
                    match (left_val.as_i64(), right_val.as_i64()) {
                        (Some(l), Some(r)) => l < r,
                        _ => false,
                    }
                },
                CompareOp::Ge => {
                    match (left_val.as_i64(), right_val.as_i64()) {
                        (Some(l), Some(r)) => l >= r,
                        _ => false,
                    }
                },
                CompareOp::Le => {
                    match (left_val.as_i64(), right_val.as_i64()) {
                        (Some(l), Some(r)) => l <= r,
                        _ => false,
                    }
                },
            };

            Value::Bool(result)
        },
        _ => Value::Null,
    }
}

/// 统一的表达式求值函数 - 所有节点处理的入口
/// 这个函数保持简洁，只负责分发到具体的节点处理函数
fn eval_expr(expr: &Expr, ctx: &EvalContext, registry: &FunctionRegistry) -> Value {
    match expr {
        Expr::Field(_) => eval_field(expr, ctx),
        Expr::StringLiteral(_) | Expr::NumberLiteral(_) => eval_literal(expr),
        Expr::FuncCall { .. } => eval_call(expr, ctx, registry),
        Expr::Compare { .. } => eval_compare(expr, ctx, registry),
    }
}

// ==========================================
// 同步引擎
// ==========================================

/// 同步引擎：管理映射配置和求值
pub struct SyncEngine {
    mappings: HashMap<String, Expr>,
    evaluator: Evaluator,
}

impl SyncEngine {
    pub fn new(configs: Vec<(String, String)>) -> Self {
        let mut mappings = HashMap::new();
        for (target_field, rule) in configs {
            match super::parser::compile_dsl(&rule) {
                Ok(ast) => {
                    mappings.insert(target_field, ast);
                },
                Err(e) => eprintln!("❌ 规则 '{}' 编译失败: {}", rule, e),
            }
        }

        Self {
            mappings,
            evaluator: Evaluator::new(),
        }
    }

    /// 处理一行数据
    pub fn process_row(&self, source_row: &Value) -> HashMap<String, Value> {
        let ctx = EvalContext::new(source_row);
        let mut target_row = HashMap::new();

        for (target_field, ast) in &self.mappings {
            let result = self.evaluator.eval(ast, &ctx);
            target_row.insert(target_field.clone(), result);
        }

        target_row
    }

    /// 获取求值器的可变引用（用于注册自定义函数）
    pub fn evaluator_mut(&mut self) -> &mut Evaluator {
        &mut self.evaluator
    }
}
