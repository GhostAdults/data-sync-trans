//! Built-in DSL function implementations.
//!
//! All functions use the same callable shape so they can be registered in the
//! DSL function registry.
//!
//! Function arguments are received as unevaluated expressions. The `eval_fn`
//! callback evaluates an expression only when the function needs its value,
//! which allows functions such as `if` to use lazy branch evaluation.

use anyhow::Result;
use serde_json::Value;

pub fn op_upper(
    args: &[super::ast::Expr],
    eval_fn: &dyn Fn(&super::ast::Expr) -> Result<Value>,
) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }
    let val = eval_fn(&args[0])?;
    let s = val.as_str().unwrap_or("");
    Ok(Value::String(s.to_uppercase()))
}

pub fn op_concat(
    args: &[super::ast::Expr],
    eval_fn: &dyn Fn(&super::ast::Expr) -> Result<Value>,
) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::Null);
    }
    let val1 = eval_fn(&args[0])?;
    let val2 = eval_fn(&args[1])?;
    let s1 = val1.as_str().unwrap_or("");
    let s2 = val2.as_str().unwrap_or("");
    Ok(Value::String(format!("{}{}", s1, s2)))
}

pub fn op_coalesce(
    args: &[super::ast::Expr],
    eval_fn: &dyn Fn(&super::ast::Expr) -> Result<Value>,
) -> Result<Value> {
    for arg in args {
        let val = eval_fn(arg)?;
        if !val.is_null() {
            return Ok(val);
        }
    }
    Ok(Value::Null)
}

/// Evaluates `if(condition, true_value, false_value)`.
///
/// This function evaluates lazily: only the selected branch is evaluated.
pub fn op_if(
    args: &[super::ast::Expr],
    eval_fn: &dyn Fn(&super::ast::Expr) -> Result<Value>,
) -> Result<Value> {
    if args.len() != 3 {
        return Ok(Value::Null);
    }

    // Evaluate the condition first.
    let condition = eval_fn(&args[0])?;

    // Evaluate only the selected branch.
    if condition.as_bool().unwrap_or(false) {
        eval_fn(&args[1])
    } else {
        eval_fn(&args[2])
    }
}

// Example for adding a new function:
// pub fn op_md5(args: &[super::ast::Expr], eval_fn: &dyn Fn(&super::ast::Expr) -> Value) -> Value {
//     if args.is_empty() {
//         return Value::Null;
//     }
//     let val = eval_fn(&args[0]);
//     let s = val.as_str().unwrap_or("");
//     let digest = md5::compute(s.as_bytes());
//     Value::String(format!("{:x}", digest))
// }
