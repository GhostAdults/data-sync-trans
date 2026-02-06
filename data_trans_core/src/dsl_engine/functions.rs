use serde_json::Value;

/// DSL 函数实现模块
/// 所有函数都使用统一的签名，便于注册到 Registry

/// 函数签名：接受参数列表和求值闭包，返回结果
/// args: 未求值的表达式参数
/// eval_fn: 求值闭包，用于按需求值参数

pub fn op_upper(args: &[super::ast::Expr], eval_fn: &dyn Fn(&super::ast::Expr) -> Value) -> Value {
    if args.is_empty() {
        return Value::Null;
    }
    let val = eval_fn(&args[0]);
    let s = val.as_str().unwrap_or("");
    Value::String(s.to_uppercase())
}

pub fn op_concat(args: &[super::ast::Expr], eval_fn: &dyn Fn(&super::ast::Expr) -> Value) -> Value {
    if args.len() < 2 {
        return Value::Null;
    }
    let val1 = eval_fn(&args[0]);
    let val2 = eval_fn(&args[1]);
    let s1 = val1.as_str().unwrap_or("");
    let s2 = val2.as_str().unwrap_or("");
    Value::String(format!("{}{}", s1, s2))
}

pub fn op_coalesce(args: &[super::ast::Expr], eval_fn: &dyn Fn(&super::ast::Expr) -> Value) -> Value {
    for arg in args {
        let val = eval_fn(arg);
        if !val.is_null() {
            return val;
        }
    }
    Value::Null
}

/// If 函数：if(condition, true_value, false_value)
/// 惰性求值：只求值被选中的分支
pub fn op_if(args: &[super::ast::Expr], eval_fn: &dyn Fn(&super::ast::Expr) -> Value) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }

    // 先求值条件表达式
    let condition = eval_fn(&args[0]);

    // 根据条件决定求值哪个分支（惰性求值）
    if condition.as_bool().unwrap_or(false) {
        eval_fn(&args[1])  // true 分支
    } else {
        eval_fn(&args[2])  // false 分支
    }
}

// 新增函数示例：
// pub fn op_md5(args: &[super::ast::Expr], eval_fn: &dyn Fn(&super::ast::Expr) -> Value) -> Value {
//     if args.is_empty() {
//         return Value::Null;
//     }
//     let val = eval_fn(&args[0]);
//     let s = val.as_str().unwrap_or("");
//     let digest = md5::compute(s.as_bytes());
//     Value::String(format!("{:x}", digest))
// }
