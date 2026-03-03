use serde_json::Value;
use std::collections::HashMap;
use super::ast::Expr;
use super::functions::*;

/// 函数签名：接受参数列表和求值闭包，返回结果
pub type DslFunction = fn(&[Expr], &dyn Fn(&Expr) -> Value) -> Value;

/// 函数注册表
pub struct FunctionRegistry {
    functions: HashMap<String, DslFunction>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };

        // 注册所有内置函数
        registry.register("upper", op_upper);
        registry.register("concat", op_concat);
        registry.register("coalesce", op_coalesce);
        registry.register("if", op_if);

        registry
    }

    /// 注册新函数
    pub fn register(&mut self, name: &str, func: DslFunction) {
        self.functions.insert(name.to_string(), func);
    }

    /// 调用函数
    pub fn call(&self, name: &str, args: &[Expr], eval_fn: &dyn Fn(&Expr) -> Value) -> Result<Value, String> {
        match self.functions.get(name) {
            Some(func) => Ok(func(args, eval_fn)),
            None => Err(format!("未知函数: {}", name)),
        }
    }

    /// 检查函数是否存在
    pub fn has_function(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
