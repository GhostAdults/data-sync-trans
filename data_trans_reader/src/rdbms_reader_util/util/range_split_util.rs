use num_bigint::BigInt;
use num_traits::Zero;

use crate::rdbms_reader_util::util::reader_split_util::PkRange;

/// 将字符串映射为 base-radix 大整数
///
/// 每个字节按 ASCII 值，高位在前：`result = result * radix + byte`
fn string_to_bigint(s: &str, radix: u32) -> BigInt {
    let base = BigInt::from(radix);
    let mut result = BigInt::zero();
    for byte in s.bytes() {
        result = result * &base + BigInt::from(byte);
    }
    result
}

/// 将大整数反向转换为字符串
///
/// 除 radix 取余还原字符，radix ∈ [1, 128]
fn bigint_to_string(n: &BigInt, radix: u32) -> String {
    if n.is_zero() {
        return String::new();
    }
    let base = BigInt::from(radix);
    let mut n = n.clone();
    let mut bytes = Vec::new();
    while !n.is_zero() {
        let remainder = &n % &base;
        let byte = remainder.to_u64_digits().1[0] as u8;
        bytes.push(byte);
        n = &n / &base;
    }
    bytes.reverse();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// 在两个大整数之间做 n 等分，返回 n+1 个切点
fn do_bigint_split(min: &BigInt, max: &BigInt, n: usize) -> Vec<BigInt> {
    if n <= 1 || min >= max {
        return vec![min.clone(), max.clone()];
    }
    let range = max - min;
    let step = &range / BigInt::from(n);
    let mut points = Vec::with_capacity(n + 1);
    for i in 0..n {
        points.push(min + &step * BigInt::from(i));
    }
    points.push(max.clone());
    points
}

/// 基于 ASCII 字典序的字符串主键范围切分
///
/// 将 `[min, max]` 的字符串范围按 advice_number 切分为若干子范围，
/// 通过 base-radix 大整数映射实现均匀切分，适用于 UUID、ULID 等随机字符串主键。
pub fn do_ascii_string_split(
    min: &str,
    max: &str,
    pk: &str,
    advice_number: usize,
    radix: u32,
) -> Vec<PkRange> {
    if advice_number <= 1 || min >= max {
        return vec![PkRange::Range {
            pk: pk.to_string(),
            min: min.to_string(),
            max: max.to_string(),
        }];
    }

    let min_int = string_to_bigint(min, radix);
    let max_int = string_to_bigint(max, radix);

    let cut_points = do_bigint_split(&min_int, &max_int, advice_number);

    let mut str_points: Vec<String> = Vec::with_capacity(cut_points.len());
    for (i, point) in cut_points.iter().enumerate() {
        if i == 0 {
            str_points.push(min.to_string());
        } else if i == cut_points.len() - 1 {
            str_points.push(max.to_string());
        } else {
            str_points.push(bigint_to_string(point, radix));
        }
    }

    let mut ranges = Vec::with_capacity(advice_number);
    for i in 0..str_points.len() - 1 {
        if i == str_points.len() - 2 {
            ranges.push(PkRange::Inclusive {
                pk: pk.to_string(),
                min: str_points[i].clone(),
                max: str_points[i + 1].clone(),
            });
        } else {
            ranges.push(PkRange::Range {
                pk: pk.to_string(),
                min: str_points[i].clone(),
                max: str_points[i + 1].clone(),
            });
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_to_bigint_roundtrip() {
        let radix = 128;
        let s = "abc";
        let n = string_to_bigint(s, radix);
        let recovered = bigint_to_string(&n, radix);
        assert_eq!(recovered, s);
    }

    #[test]
    fn test_do_bigint_split_basic() {
        let min = BigInt::from(0);
        let max = BigInt::from(100);
        let points = do_bigint_split(&min, &max, 4);
        assert_eq!(points.len(), 5);
        assert_eq!(points[0], BigInt::from(0));
        assert_eq!(points[4], BigInt::from(100));
    }

    #[test]
    fn test_ascii_string_split_ranges() {
        let ranges = do_ascii_string_split("a", "z", "id", 4, 128);
        assert_eq!(ranges.len(), 4);
        // 首区间 min 应为 "a"
        match &ranges[0] {
            PkRange::Range { min, .. } => assert_eq!(min, "a"),
            _ => panic!("expected Range"),
        }
        // 末区间应为 Inclusive，max 为 "z"
        match &ranges[3] {
            PkRange::Inclusive { max, .. } => assert_eq!(max, "z"),
            _ => panic!("expected Inclusive"),
        }
    }
}
