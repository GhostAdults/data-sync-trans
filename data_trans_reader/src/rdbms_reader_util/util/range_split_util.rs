use num_bigint::{BigInt, BigUint};
use num_traits::{Num, Zero};

use crate::rdbms_reader_util::util::reader_split_util::PkRange;

fn uuid_to_biguint(s: &str) -> BigUint {
    BigUint::from_str_radix(s, 16).expect("invalid uuid hex string")
}

fn biguint_to_uuid(n: &BigUint) -> String {
    // 固定 32 位，保证字典序 == 数值序
    format!("{:032x}", n)
}

/// 字符串 → 大整数（
///
///  低位在前 rev
/// 从右到左遍历，`result = result + byte * radix^k`
fn string_to_bigint(s: &str, radix: u32) -> BigInt {
    let base = BigInt::from(radix);
    let mut result = BigInt::zero();
    let mut k: u32 = 0;
    for b in s.bytes().rev() {
        assert!(
            b < 128,
            "仅支持 ASCII 字符串，radix到非 ASCII 字符: 0x{:02X}",
            b
        );
        result = result + BigInt::from(b) * base.pow(k);
        k += 1;
    }
    result
}

/// 大整数 → 字符串（radix 进制逐位取余还原）
fn bigint_to_string(n: &BigInt, radix: u32) -> String {
    let base = BigInt::from(radix);
    let mut current = n.clone();
    let mut list = Vec::new();

    let mut quotient = &current / &base;
    while quotient > BigInt::zero() {
        list.push(&current % &base);
        current = &current / &base;
        quotient = current.clone(); // Java: quotient = currentValue
    }

    if list.is_empty() {
        list.insert(0, n % &base);
    }

    list.reverse();

    let mut bytes = Vec::with_capacity(list.len());
    for d in &list {
        let v = d.to_u64_digits().1.first().copied().unwrap_or(0) as u8;
        bytes.push(v);
    }

    String::from_utf8_lossy(&bytes).into_owned()
}

/// 大整数区间 n 等分，返回 n+1 个切点
/// - step = gap / n，remainder = gap % n
/// - 若 step == 0，则实际份数 = remainder
/// - 切点 = left + step * i + min(i, remainder) （remainder 均摊到前 remainder 个区间）
fn do_bigint_split(left: &BigInt, right: &BigInt, n: usize) -> Vec<BigInt> {
    assert!(n >= 1, "切分份数不能小于 1");

    if left == right {
        return vec![left.clone(), right.clone()];
    }

    let (left, right) = if left > right {
        (right.clone(), left.clone())
    } else {
        (left.clone(), right.clone())
    };

    let gap = &right - &left;
    let step = &gap / BigInt::from(n as u64);
    let remainder = (&gap % BigInt::from(n as u64))
        .to_u64_digits()
        .1
        .first()
        .copied()
        .unwrap_or(0) as usize;

    let n = if step.is_zero() { remainder } else { n };

    if n == 0 {
        return vec![left.clone(), right.clone()];
    }

    let mut result = Vec::with_capacity(n + 1);
    result.push(left.clone());

    let mut upper_bound = left.clone();
    for i in 1..n {
        upper_bound = upper_bound + &step;
        if remainder >= i {
            upper_bound = upper_bound + BigInt::from(1u32);
        }
        result.push(upper_bound.clone());
    }

    result.push(right);
    result
}

/// 基于 ASCII 字典序的字符串主键范围切分
///
/// 将 `[min, max]` 的字符串范围按 advice_number 切分为若干子范围，
/// 通过 radix=128 大整数映射实现均匀切分
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

    let mut str_points = Vec::with_capacity(cut_points.len());
    for (i, point) in cut_points.iter().enumerate() {
        if i == 0 {
            str_points.push(min.to_string());
        } else if i == cut_points.len() - 1 {
            str_points.push(max.to_string());
        } else {
            str_points.push(bigint_to_string(point, radix));
        }
    }

    let mut ranges = Vec::with_capacity(str_points.len() - 1);
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

/// 基于 Hex 字典序的字符串主键范围切分
/// 字符串转化成16进制数字进行切分，适用于 UUID、ULID 等随机字符串主键。
/// 将 `[min, max]` 的字符串范围按 advice_number 切分为若干子范围,
///
pub fn do_hex_string_split(
    left: &str,
    right: &str,
    pk: &str,
    n: usize, // advice_number
    _radix: u32,
) -> Vec<PkRange> {
    assert!(n > 0);

    let mut l = uuid_to_biguint(left);
    let mut r = uuid_to_biguint(right);

    if l > r {
        std::mem::swap(&mut l, &mut r);
    }

    let gap = &r - &l;
    let n_big = BigUint::from(n as u64);

    let step = &gap / &n_big;
    let remainder = (&gap % &n_big)
        .to_u64_digits()
        .first()
        .copied()
        .unwrap_or(0) as usize;
    let mut ranges = Vec::with_capacity(n);

    let mut current = l.clone();

    for i in 0..n {
        let mut next = &current + &step;

        // 把余数均摊到前 remainder 段（关键点）
        if i < remainder {
            next += BigUint::from(1u32);
        }

        //  如果 inclusive_end: i == n - 1,则push Inclusive，否则push Range
        if i == n - 1 {
            // 最后一段强制闭合
            next = r.clone();
            ranges.push(PkRange::Inclusive {
                pk: pk.to_string(),
                min: biguint_to_uuid(&current),
                max: biguint_to_uuid(&next),
            });
        } else {
            ranges.push(PkRange::Range {
                pk: pk.to_string(),
                min: biguint_to_uuid(&current),
                max: biguint_to_uuid(&next),
            });
        }
        current = next;
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
    fn test_uuid_roundtrip() {
        let radix = 128;
        let uuid = "3ee34c4a91e011efbd66058b2b218de3";
        let n = string_to_bigint(uuid, radix);
        let recovered = bigint_to_string(&n, radix);
        assert_eq!(recovered, uuid);
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
    fn test_do_hex_string_split() {
        let ranges = do_hex_string_split(
            "3ee34c4a91e011efbd66058b2b218de3",
            "fd6c65aa29024bdea7b20c987684584a",
            "id",
            10,
            128,
        );

        for (i, r) in ranges.iter().enumerate() {
            println!(
                "Range {}: min = {}, max = {}",
                i,
                match r {
                    PkRange::Range { min, .. } => min,
                    PkRange::Inclusive { min, .. } => min,
                },
                match r {
                    PkRange::Range { max, .. } => max,
                    PkRange::Inclusive { max, .. } => max,
                }
            );
        }
    }

    #[test]
    fn test_ascii_split_real() {
        let ranges = do_ascii_string_split(
            "3ee34c4a91e011efbd66058b2b218de3",
            "fd6c65aa29024bdea7b20c987684584a",
            "id",
            10,
            128,
        );
        eprintln!("\n=== UUID split: 10 way ===");
        for (i, range) in ranges.iter().enumerate() {
            let (min, max) = match range {
                PkRange::Range { min, max, .. } => (min, max),
                PkRange::Inclusive { min, max, .. } => (min, max),
            };
            let op = if matches!(range, PkRange::Range { .. }) {
                "<"
            } else {
                "<="
            };
            eprintln!("[task{:02}] id >= '{}' AND id {} '{}'", i, min, op, max);
        }
        eprintln!("=== total {} ranges ===\n", ranges.len());
    }
}
