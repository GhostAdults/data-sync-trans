use anyhow::Result;
use anyhow::{bail, Context};
use relus_common::job_config::JobConfig;
use relus_common::ApiConfig;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::process::Command;

pub fn try_curl(cfg: &ApiConfig) -> Result<Vec<u8>> {
    let mut args: Vec<String> = Vec::new();
    args.push("-sS".to_string());
    if let Some(t) = cfg.timeout_secs {
        args.push("--max-time".to_string());
        args.push(t.to_string());
    }
    let method = cfg.method.as_deref().unwrap_or("GET").to_uppercase();
    args.push("-X".to_string());
    args.push(method.clone());
    if let Some(hs) = &cfg.headers {
        for (k, v) in hs {
            args.push("-H".to_string());
            args.push(format!("{}: {}", k, v));
        }
    }
    if let Some(b) = &cfg.body {
        let s = serde_json::to_string(b)?;
        args.push("-d".to_string());
        args.push(s.clone());
    }
    args.push(cfg.url.clone());
    let output = Command::new("curl").args(&args).output();
    match output {
        Ok(out) => {
            if !out.status.success() {
                bail!("curl 请求失败，状态码: {:?}", out.status.code());
            }
            Ok(out.stdout)
        }
        Err(e) => bail!("无法执行 curl: {}", e),
    }
}

pub fn try_powershell(cfg: &ApiConfig) -> Result<Vec<u8>> {
    let method = cfg.method.as_deref().unwrap_or("GET").to_uppercase();
    let mut ps = String::new();
    ps.push_str("$hdr=@{};");
    if let Some(hs) = &cfg.headers {
        for (k, v) in hs {
            ps.push_str(&format!(
                "$hdr['{}']='{}';",
                k.replace("'", "''"),
                v.replace("'", "''")
            ));
        }
    }
    let body = if let Some(b) = &cfg.body {
        let s = serde_json::to_string(b)?;
        format!(
            " -ContentType 'application/json' -Body @'{0}'@ ",
            s.replace("'", "''")
        )
    } else {
        "".to_string()
    };
    let timeout = cfg.timeout_secs.unwrap_or(30);
    ps.push_str(&format!(
        "$r=Invoke-RestMethod -Uri '{}' -Method {} -Headers $hdr{} -TimeoutSec {}; $r | ConvertTo-Json -Depth 20",
        cfg.url.replace("'", "''"),
        method,
        body,
        timeout
    ));
    let output = Command::new("powershell")
        .args(&["-NoProfile", "-Command", &ps])
        .output();
    match output {
        Ok(out) => {
            if !out.status.success() {
                bail!(
                    "powershell Invoke-RestMethod 失败，状态码: {:?}",
                    out.status.code()
                );
            }
            Ok(out.stdout)
        }
        Err(e) => bail!("无法执行 powershell: {}", e),
    }
}

// fetch json data from api
pub async fn fetch_json(cfg: &ApiConfig) -> Result<JsonValue> {
    let data = match try_curl(cfg) {
        Ok(d) => d,
        Err(_) => try_powershell(cfg)?,
    };
    let v: JsonValue = serde_json::from_slice(&data)?;
    Ok(v)
}

/// 从 API 获取数据
pub async fn fetch_from_api(config: &JobConfig) -> Result<Vec<JsonValue>> {
    let api_config = config.source.parse_api_config()?;
    let resp = fetch_json(&api_config).await?;
    let items = extract_items(&resp, &api_config.items_json_path)?;
    Ok(items)
}

pub async fn fetch_json_by_config(
    config: &ApiConfig, // HttpRequestConfig
) -> Result<Vec<JsonValue>> {
    fetch_json_data(
        &config.url,
        config.method.as_deref(),
        config.headers.as_ref(),
        config.body.as_ref(),
        config.items_json_path.as_deref(),
    )
    .await
}

pub async fn fetch_json_data(
    url: &str,
    method: Option<&str>,
    headers: Option<&BTreeMap<String, String>>,
    body: Option<&JsonValue>,
    items_json_path: Option<&str>,
) -> Result<Vec<JsonValue>> {
    let client = reqwest::Client::new();
    let method = method.unwrap_or("GET").to_uppercase();

    let mut request = match method.as_str() {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "DELETE" => client.delete(url),
        _ => bail!("不支持的 HTTP 方法: {}", method),
    };

    // 设置 header
    if let Some(headers) = headers {
        for (k, v) in headers {
            request = request.header(k, v);
        }
    }

    // 设置 body（JSON）
    if let Some(body) = body {
        request = request.json(body);
    }

    let response = request.send().await?;
    let json: JsonValue = response.json().await?;

    // 提取 items
    let items = if let Some(path) = items_json_path {
        extract_by_path(&json, path).unwrap_or(&json)
    } else {
        &json
    };

    match items {
        JsonValue::Array(arr) => Ok(arr.clone()),
        _ => Ok(vec![items.clone()]),
    }
}

/// 从响应中提取数据项数组
pub fn extract_items(resp: &JsonValue, items_path: &Option<String>) -> Result<Vec<JsonValue>> {
    if let Some(path) = items_path {
        let v = extract_by_path(resp, path).context("未找到 items_json_path 指定的数据")?;
        match v {
            JsonValue::Array(a) => Ok(a.clone()),
            _ => bail!("items_json_path 需指向数组"),
        }
    } else {
        match resp {
            JsonValue::Array(a) => Ok(a.clone()),
            JsonValue::Object(o) => {
                for (_k, v) in o {
                    if let JsonValue::Array(a) = v {
                        return Ok(a.clone());
                    }
                }
                bail!("响应顶层不是数组，且未提供 items_json_path")
            }
            _ => bail!("响应不是数组或对象"),
        }
    }
}

/// 提取 JSON 路径
pub fn extract_by_path<'a>(root: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path == "/" || path.is_empty() {
        return Some(root);
    }
    if path.starts_with('/') {
        return root.pointer(path);
    }
    let mut cur = root;
    for seg in path.split('.') {
        match cur {
            JsonValue::Object(map) => {
                cur = map.get(seg)?;
            }
            JsonValue::Array(arr) => {
                if let Ok(idx) = seg.parse::<usize>() {
                    cur = arr.get(idx)?;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(cur)
}
