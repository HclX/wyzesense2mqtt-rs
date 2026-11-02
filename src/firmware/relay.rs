//! Updating a bridged OpenWyzeSense relay (an ESP32, ESP32-S3 or ESP8266
//! next to a contact board) with its own firmware, over Wi-Fi.
//!
//! The relay's web server is at the address its WebSocket connects from. Its
//! `/api/status` names its chip and build and says whether it has a
//! password; `/ota` takes the image (a multipart field `image`), writes it to
//! the unused slot, and restarts. A new build that doesn't rejoin Wi-Fi rolls
//! back. On start-up the relay brings its contact board's radio up to the
//! gateway firmware it carries, then reconnects to the bridge.

use super::{Firmware, Job, JobKind, Step};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// How long a relay status is reused.
const STATUS_TTL: Duration = Duration::from_secs(10);
/// How long a restarting relay is waited for.
const RESTART_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// What a relay says about itself.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RelayInfo {
    pub name: String,
    /// "ESP32", "ESP32-S3" or "ESP8266".
    pub chip: String,
    /// Its build, e.g. "v0.2.0 (ota_0)".
    pub build: String,
    /// Whether it asks for a password.
    pub auth: bool,
    /// The release file for it.
    pub product: Option<String>,
    /// Its web address.
    pub url: String,
    /// Seconds since it started.
    #[serde(skip)]
    pub uptime: Option<u64>,
}

impl RelayInfo {
    /// The firmware slot it runs from, e.g. "ota_0" in "v0.2.0 (ota_0)".
    pub fn slot(&self) -> Option<&str> {
        let start = self.build.rfind('(')?;
        self.build[start + 1..].strip_suffix(')')
    }
}

/// The release file that updates a relay with this chip.
pub fn relay_product(chip: &str) -> Option<&'static str> {
    match chip {
        "ESP32" => Some("relay-esp32-ota"),
        "ESP32-S3" => Some("relay-esp32s3-ota"),
        "ESP8266" => Some("relay-esp8266"),
        _ => None,
    }
}

/// The relay's own version marker in its image (`OWS-RELAY-VERSION:<v>`),
/// distinct from the version of the gateway image it carries.
pub fn relay_image_version(image: &[u8]) -> Option<String> {
    const MARK: &[u8] = b"OWS-RELAY-VERSION:";
    let at = image.windows(MARK.len()).position(|w| w == MARK)? + MARK.len();
    let end = image[at..].iter().position(|&b| b == 0 || b.is_ascii_whitespace()).map_or(image.len(), |n| at + n);
    Some(String::from_utf8_lossy(&image[at..end.min(at + 64)]).to_string())
}

/// "192.168.1.5:51234" or "[fe80::1]:51234" -> the host part.
fn host_of(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    addr.rsplit_once(':').map_or(addr, |(h, _)| h)
}

impl Firmware {
    fn relay_url(&self, addr: &str) -> String {
        let host = host_of(addr);
        let host = if host.contains(':') { format!("[{}]", host) } else { host.to_string() };
        if self.relay_port == 80 { format!("http://{}", host) } else { format!("http://{}:{}", host, self.relay_port) }
    }

    /// The relay behind a bridge connection from `addr`, if it is one of ours.
    pub(super) async fn relay_info(&self, addr: Option<&str>) -> Option<RelayInfo> {
        let addr = addr?;
        if let Some((at, info)) = self.relay_cache.lock().unwrap().get(addr) {
            if at.elapsed() < STATUS_TTL {
                return info.clone();
            }
        }
        let info = self.fetch_relay_info(addr).await;
        self.relay_cache.lock().unwrap().insert(addr.to_string(), (Instant::now(), info.clone()));
        info
    }

    async fn fetch_relay_info(&self, addr: &str) -> Option<RelayInfo> {
        let url = self.relay_url(addr);
        let status: serde_json::Value = reqwest::Client::new()
            .get(format!("{}/api/status", url))
            .timeout(Duration::from_secs(3))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        let chip = status["chip"].as_str()?.to_string();
        Some(RelayInfo {
            name: status["name"].as_str().unwrap_or("").to_string(),
            build: status["build"].as_str().unwrap_or("").to_string(),
            auth: status["auth"].as_bool().unwrap_or(false),
            product: relay_product(&chip).map(str::to_string),
            chip,
            url,
            uptime: status["uptime"].as_u64(),
        })
    }

    /// Updates a bridged relay to [`RELEASE_VERSION`]. `password` is its admin
    /// password, when it has one (the bridge token is tried otherwise).
    pub async fn start_relay_update(self: &Arc<Self>, mac: &str, password: Option<String>) -> Result<Job, String> {
        let addr = {
            let map = self.engines.lock().await;
            let engine = map.get(mac).ok_or_else(|| format!("dongle {} is not connected", mac))?;
            engine.remote_addr.clone().ok_or("only a bridged relay updates over Wi-Fi")?
        };
        let info = self.fetch_relay_info(&addr).await.ok_or("the gateway's address doesn't answer as an OpenWyzeSense relay")?;
        let product = info.product.clone().ok_or_else(|| format!("no relay firmware for a {}", info.chip))?;
        let secret = password.filter(|p| !p.is_empty()).or_else(|| self.bridge_token.clone());
        if info.auth && secret.is_none() {
            return Err("this relay has a password: enter its admin password".into());
        }
        let job = self.begin(mac, JobKind::Relay).await?;
        self.update(|j| j.current = Some(format!("{} {}", info.chip, info.build)));
        let me = Arc::clone(self);
        let task = job.clone();
        tokio::spawn(async move {
            if let Err(e) = me.run_relay_update(&task, &info, &product, secret).await {
                me.fail(e);
            }
        });
        Ok(self.job().unwrap_or(job))
    }

    async fn run_relay_update(&self, job: &Job, info: &RelayInfo, product: &str, secret: Option<String>) -> Result<(), String> {
        let (version, file, image) = self.fetch_file(product).await?;
        let built_as = relay_image_version(&image)
            .ok_or_else(|| format!("{} is not an OpenWyzeSense relay image (no version marker)", file))?;
        if built_as != version {
            // v0.2.0's ESP32 images were built without their version ("unknown").
            warn!("{} says it was built as {:?}, not {}", file, built_as, version);
        }
        info!("Updating relay {} ({}, {}) to {}: {}", job.dongle, info.chip, info.build, version, file);
        self.update(|j| {
            j.target = Some(version.clone());
            j.step = Step::Flash;
            j.phase = Some("upload".into());
            j.done = 0;
            j.total = image.len();
        });

        // The upload, chunked so the dashboard can follow it.
        let total = image.len();
        let job_slot = Arc::clone(&self.job);
        let tx = self.broadcast_tx.clone();
        let chunks: Vec<Vec<u8>> = image.chunks(4096).map(|c| c.to_vec()).collect();
        let mut sent = 0usize;
        let mut last_pct = 0usize;
        let stream = futures_util::stream::iter(chunks.into_iter().map(move |c| {
            sent += c.len();
            if let Some(j) = job_slot.lock().unwrap().as_mut() {
                j.done = sent;
            }
            let pct = sent * 100 / total.max(1);
            if pct != last_pct {
                last_pct = pct;
                let _ = tx.send(());
            }
            Ok::<_, std::io::Error>(c)
        }));
        let part = reqwest::multipart::Part::stream_with_length(reqwest::Body::wrap_stream(stream), total as u64)
            .file_name(file.clone())
            .mime_str("application/octet-stream")
            .map_err(|e| e.to_string())?;
        let mut req = reqwest::Client::new()
            .post(format!("{}/ota", info.url))
            .timeout(Duration::from_secs(180))
            .multipart(reqwest::multipart::Form::new().part("image", part));
        if let Some(s) = &secret {
            req = req.bearer_auth(s);
        }
        let resp = req.send().await.map_err(|e| format!("uploading to the relay: {}", e))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err("the relay refused the password".into());
        }
        if !status.is_success() {
            return Err(format!("the relay refused the firmware ({}): {}", status, body.trim()));
        }
        info!("Relay {} took the firmware: {}", job.dongle, body.trim());

        // It restarts, syncs its radio, and reconnects.
        self.update(|j| {
            j.step = Step::Restart;
            j.phase = None;
            j.message = Some("The relay is restarting into the new firmware.".into());
        });
        let addr = job.device_path.clone();
        let uploaded = Instant::now();
        let deadline = uploaded + RESTART_TIMEOUT;
        let mut seen_build = None;
        tokio::time::sleep(Duration::from_secs(5)).await;
        while Instant::now() < deadline {
            if let Some(now) = self.fetch_relay_info(&addr).await {
                // Restarted since the upload, and on the new build: its version
                // says so, or (a build that doesn't know its version) it runs
                // from the other slot.
                let restarted = now.uptime.is_none_or(|u| u <= uploaded.elapsed().as_secs() + 2);
                let new_build = now.build.starts_with(&version)
                    || (now.build != info.build && now.slot().is_some() && now.slot() != info.slot());
                if restarted && new_build {
                    if self.engines.lock().await.contains_key(&job.dongle) {
                        self.relay_cache.lock().unwrap().remove(&addr);
                        info!("Relay {} is back on {}", job.dongle, now.build);
                        self.update(|j| {
                            j.step = Step::Done;
                            j.message = None;
                            j.current = Some(format!("{} {}", now.chip, now.build));
                        });
                        return Ok(());
                    }
                } else if restarted && now.build == info.build {
                    return Err(format!(
                        "the relay restarted on its old firmware ({}): the new build rolled back, \
                         which it does when it can't rejoin Wi-Fi",
                        now.build
                    ));
                } else {
                    seen_build = Some(now.build);
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        warn!("Relay {} did not come back on {}", job.dongle, version);
        Err(match seen_build {
            Some(b) => format!(
                "the relay came back on {} instead of {} (a new build that can't join Wi-Fi rolls back)",
                b, version
            ),
            None => format!("the relay didn't come back within {} minutes", RESTART_TIMEOUT.as_secs() / 60),
        })
    }
}

#[cfg(test)]
mod slot_tests {
    use super::*;

    fn info(build: &str) -> RelayInfo {
        RelayInfo {
            name: String::new(),
            chip: "ESP32".into(),
            build: build.into(),
            auth: false,
            product: None,
            url: String::new(),
            uptime: None,
        }
    }

    #[test]
    fn slots() {
        assert_eq!(info("v0.2.0 (ota_0)").slot(), Some("ota_0"));
        assert_eq!(info("unknown (ota_1)").slot(), Some("ota_1"));
        assert_eq!(info("v0.2.0").slot(), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_and_products() {
        assert_eq!(host_of("192.168.1.5:51234"), "192.168.1.5");
        assert_eq!(host_of("[fe80::1]:51234"), "fe80::1");
        assert_eq!(host_of("relay.lan"), "relay.lan");
        assert_eq!(relay_product("ESP32"), Some("relay-esp32-ota"));
        assert_eq!(relay_product("ESP32-S3"), Some("relay-esp32s3-ota"));
        assert_eq!(relay_product("ESP8266"), Some("relay-esp8266"));
        assert_eq!(relay_product("RP2040"), None);
        let mut img = vec![0xE9u8; 64];
        img.extend_from_slice(b"OWS-RELAY-VERSION:v0.2.0\0more");
        assert_eq!(relay_image_version(&img).as_deref(), Some("v0.2.0"));
        assert_eq!(relay_image_version(b"no marker"), None);
    }
}
