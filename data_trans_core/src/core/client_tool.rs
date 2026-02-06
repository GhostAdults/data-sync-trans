use serde_json::Value as JsonValue;
use crate::core::ApiConfig;
use anyhow::{Result};
use std::process::Command;
use anyhow::bail;

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
            ps.push_str(&format!("$hdr['{}']='{}';", k.replace("'", "''"), v.replace("'", "''")));
        }
    }
    let body = if let Some(b) = &cfg.body {
        let s = serde_json::to_string(b)?;
        format!(" -ContentType 'application/json' -Body @'{0}'@ ", s.replace("'", "''"))
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
    let output = Command::new("powershell").args(&["-NoProfile", "-Command", &ps]).output();
    match output {
        Ok(out) => {
            if !out.status.success() {
                bail!("powershell Invoke-RestMethod 失败，状态码: {:?}", out.status.code());
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