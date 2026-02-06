use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{multispace0, alpha1, digit1},
    multi::separated_list0,
    sequence::{delimited, tuple},
    IResult,
};
use super::ast::{Expr, CompareOp};

/// 解析字段引用：source.field_name
fn parse_field(input: &str) -> IResult<&str, Expr> {
    let (input, _) = tag("source.")(input)?;
    let (input, field_name) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    Ok((input, Expr::Field(field_name.to_string())))
}

/// 解析字符串字面量：'text'
fn parse_string_literal(input: &str) -> IResult<&str, Expr> {
    let (input, content) = delimited(tag("'"), take_until("'"), tag("'"))(input)?;
    Ok((input, Expr::StringLiteral(content.to_string())))
}

/// 解析数字字面量：123
fn parse_number_literal(input: &str) -> IResult<&str, Expr> {
    let (input, num_str) = digit1(input)?;
    let num = num_str.parse::<i64>().unwrap_or(0);
    Ok((input, Expr::NumberLiteral(num)))
}

/// 解析比较操作符
fn parse_compare_op(input: &str) -> IResult<&str, CompareOp> {
    let (input, _) = multispace0(input)?;
    let (input, op) = alt((
        tag("=="),
        tag("!="),
        tag(">="),
        tag("<="),
        tag(">"),
        tag("<"),
    ))(input)?;
    let (input, _) = multispace0(input)?;

    let compare_op = match op {
        "==" => CompareOp::Eq,
        "!=" => CompareOp::Ne,
        ">" => CompareOp::Gt,
        "<" => CompareOp::Lt,
        ">=" => CompareOp::Ge,
        "<=" => CompareOp::Le,
        _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))),
    };

    Ok((input, compare_op))
}

/// 解析函数调用：func_name(arg1, arg2, ...)
fn parse_func_call(input: &str) -> IResult<&str, Expr> {
    // 解析函数名
    let (input, func_name) = alpha1(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("(")(input)?;
    let (input, _) = multispace0(input)?;

    // 解析参数列表
    let (input, args) = separated_list0(
        tuple((multispace0, tag(","), multispace0)),
        parse_expr
    )(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = tag(")")(input)?;

    Ok((input, Expr::FuncCall {
        name: func_name.to_string(),
        args
    }))
}

/// 解析表达式
fn parse_expr(input: &str) -> IResult<&str, Expr> {
    // 尝试解析基础表达式
    let (input, left) = alt((
        parse_func_call,
        parse_field,
        parse_string_literal,
        parse_number_literal
    ))(input)?;

    // 尝试解析比较操作符
    if let Ok((input, op)) = parse_compare_op(input) {
        let (input, right) = alt((
            parse_func_call,
            parse_field,
            parse_string_literal,
            parse_number_literal
        ))(input)?;
        return Ok((input, Expr::Compare {
            left: Box::new(left),
            op,
            right: Box::new(right)
        }));
    }

    Ok((input, left))
}

/// 编译 DSL 字符串为 AST
pub fn compile_dsl(dsl: &str) -> Result<Expr, String> {
    match parse_expr(dsl.trim()) {
        Ok((remainder, expr)) => {
            if !remainder.is_empty() {
                Err(format!("语法错误，未解析部分: {}", remainder))
            } else {
                Ok(expr)
            }
        },
        Err(e) => Err(format!("解析失败: {}", e)),
    }
}
