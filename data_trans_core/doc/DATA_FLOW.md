# DSL 引擎数据流处理详解

## 概览

当数据流入 `process_row` 方法时，经历以下步骤：

```
输入数据 → 创建上下文 → 遍历映射规则 → AST 求值 → 输出结果
```

---

## 详细流程示例

### 输入数据
```json
{
  "name": "alice",
  "age": 25,
  "status": "active"
}
```

### 配置规则
```rust
vec![
    ("user_name", "upper(source.name)"),
    ("is_adult", "if(source.age >= 18, 1, 0)"),
    ("status", "source.status")
]
```

---

## 步骤 1: 初始化阶段 (SyncEngine::new)

### 1.1 解析 DSL 规则为 AST

每个规则字符串会被解析为 AST 树：

**规则 1**: `"upper(source.name)"`
```
Expr::FuncCall {
    name: "upper",
    args: [
        Expr::Field("name")
    ]
}
```

**规则 2**: `"if(source.age >= 18, 1, 0)"`
```
Expr::FuncCall {
    name: "if",
    args: [
        Expr::Compare {
            left: Box(Expr::Field("age")),
            op: CompareOp::Ge,
            right: Box(Expr::NumberLiteral(18))
        },
        Expr::NumberLiteral(1),
        Expr::NumberLiteral(0)
    ]
}
```

**规则 3**: `"source.status"`
```
Expr::Field("status")
```

### 1.2 构建映射表

```rust
mappings: HashMap {
    "user_name" => Expr::FuncCall { name: "upper", ... },
    "is_adult" => Expr::FuncCall { name: "if", ... },
    "status" => Expr::Field("status")
}
```

---

## 步骤 2: 运行时处理 (process_row)

### 2.1 创建求值上下文

```rust
let ctx = EvalContext::new(source_row);
// ctx.source = { "name": "alice", "age": 25, "status": "active" }
```

### 2.2 遍历映射规则并求值

对于每个映射规则 `(target_field, ast)`，调用 `evaluator.eval(ast, &ctx)`

---

## 步骤 3: AST 求值详解

### 示例 1: 处理 `"user_name" => upper(source.name)`

**调用链路**:

1. **Evaluator.eval()** - 统一入口
   ```rust
   match expr {
       Expr::FuncCall { .. } => eval_call(expr, ctx, &self.registry)
   }
   ```

2. **eval_call()** - 处理函数调用节点
   ```rust
   // 提取函数信息
   name = "upper"
   args = [Expr::Field("name")]

   // 创建求值闭包
   let eval_fn = |arg_expr: &Expr| eval_expr(arg_expr, ctx, registry);

   // 调用注册表
   registry.call("upper", args, &eval_fn)
   ```

3. **FunctionRegistry.call()** - 查找并调用函数
   ```rust
   // 从 HashMap 中查找 "upper" 函数
   let func = self.functions.get("upper")?;

   // 调用函数（传入未求值的参数和求值闭包）
   func(args, &eval_fn)
   ```

4. **op_upper()** - 执行具体的函数逻辑
   ```rust
   // 求值第一个参数
   let val = eval_fn(&args[0]);
   // 这会触发：eval_expr(Expr::Field("name"), ctx, registry)
   //          → eval_field(Expr::Field("name"), ctx)
   //          → ctx.source.get("name")
   //          → Value::String("alice")

   // 转换为字符串
   let s = val.as_str().unwrap_or(""); // "alice"

   // 转大写
   Value::String(s.to_uppercase()) // "ALICE"
   ```

**结果**: `"user_name" => "ALICE"`

---

### 示例 2: 处理 `"is_adult" => if(source.age >= 18, 1, 0)`

**调用链路**:

1. **Evaluator.eval()** - 识别为函数调用
   ```rust
   Expr::FuncCall { name: "if", args: [...] } => eval_call(...)
   ```

2. **eval_call()** - 准备调用 if 函数
   ```rust
   name = "if"
   args = [
       Expr::Compare { left: Field("age"), op: Ge, right: NumberLiteral(18) },
       Expr::NumberLiteral(1),
       Expr::NumberLiteral(0)
   ]

   registry.call("if", args, &eval_fn)
   ```

3. **op_if()** - 执行条件判断（惰性求值）
   ```rust
   // 先求值条件表达式（第一个参数）
   let condition = eval_fn(&args[0]);
   // 这会触发：
   //   eval_expr(Expr::Compare {...}, ctx, registry)
   //   → eval_compare(...)
   //     → left_val = eval_expr(Field("age"), ...) = 25
   //     → right_val = eval_expr(NumberLiteral(18), ...) = 18
   //     → 25 >= 18 = true
   //     → Value::Bool(true)

   // 根据条件选择分支
   if condition.as_bool().unwrap_or(false) {
       eval_fn(&args[1])  // 求值 true 分支
       // → eval_expr(NumberLiteral(1), ...) = Value::Number(1)
   } else {
       eval_fn(&args[2])  // false 分支（未执行）
   }
   ```

**结果**: `"is_adult" => 1`

---

### 示例 3: 处理 `"status" => source.status`

**调用链路**:

1. **Evaluator.eval()** - 识别为字段引用
   ```rust
   Expr::Field("status") => eval_field(expr, ctx)
   ```

2. **eval_field()** - 直接从源数据中读取
   ```rust
   match expr {
       Expr::Field(key) => ctx.source.get(key).cloned().unwrap_or(Value::Null)
       // ctx.source.get("status") = Some(Value::String("active"))
   }
   ```

**结果**: `"status" => "active"`

---

## 步骤 4: 汇总结果

```rust
target_row: HashMap {
    "user_name" => Value::String("ALICE"),
    "is_adult" => Value::Number(1),
    "status" => Value::String("active")
}
```

---

## 关键特性

### 1. **惰性求值**
函数参数不会立即求值，而是传递 AST 节点和求值闭包，由函数决定何时求值。

例如 `if(condition, true_value, false_value)` 只会求值被选中的分支。

### 2. **递归求值**
函数可以嵌套调用，每次通过 `eval_fn` 递归求值参数。

例如 `upper(concat(source.first, source.last))`:
```
eval_call("upper", [concat(...)])
  → op_upper 调用 eval_fn(concat(...))
    → eval_call("concat", [Field("first"), Field("last")])
      → op_concat 调用 eval_fn(Field("first")) 和 eval_fn(Field("last"))
        → eval_field → "John", "Doe"
      → 返回 "JohnDoe"
  → 返回 "JOHNDOE"
```

### 3. **类型安全**
通过 AST 枚举实现编译时类型检查：
- `Expr::Field` - 字段引用
- `Expr::StringLiteral` / `Expr::NumberLiteral` - 字面量
- `Expr::FuncCall` - 函数调用
- `Expr::Compare` - 比较表达式

### 4. **模块化求值**
每种 AST 节点都有独立的求值函数：
- `eval_literal()` - 处理字面量
- `eval_field()` - 处理字段引用
- `eval_call()` - 处理函数调用
- `eval_compare()` - 处理比较表达式

---

## 性能考虑

1. **AST 预编译**: 规则在初始化时解析一次，运行时直接执行 AST
2. **零拷贝**: 使用引用传递，避免不必要的数据拷贝
3. **HashMap 查找**: 函数注册表使用 HashMap，O(1) 查找时间
4. **惰性求值**: 避免执行不需要的分支

---

## 扩展性

新增函数只需 3 步：

1. **实现函数** (functions.rs)
   ```rust
   pub fn op_md5(args: &[Expr], eval_fn: &dyn Fn(&Expr) -> Value) -> Value {
       let val = eval_fn(&args[0]);
       let s = val.as_str().unwrap_or("");
       Value::String(format!("{:x}", md5::compute(s)))
   }
   ```

2. **注册函数** (registry.rs)
   ```rust
   registry.register("md5", super::functions::op_md5);
   ```

3. **使用函数**
   ```rust
   ("hash", "md5(source.password)")
   ```

无需修改 `evaluator.rs` 或任何其他代码！
