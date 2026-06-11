//! Battery voltage-to-capacity conversion curves.
//!
//! The raw battery byte from Wyze sensors represents `AON_BATMON:BAT >> 3`,
//! where BAT is the CC1310's battery monitor register in INT.FRAC format
//! (bits[10:8] = integer volts 0–4, bits[7:0] = fractional 1/256V).
//! Therefore: `voltage = raw / 32.0` (i.e. 0.03125V per unit).
//!
//! Since battery discharge curves are non-linear (especially lithium coin
//! cells which maintain a voltage plateau then drop steeply), reporting the raw
//! value as a percentage is misleading — a sensor can show "70%" and die shortly
//! after.
//!
//! This module provides per-chemistry piecewise-linear discharge curves to convert
//! the voltage-proportional raw byte into an approximate remaining capacity percentage.

use crate::protocol::telemetry::SensorType;

/// Battery chemistry categories, each with a distinct discharge curve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatteryChemistry {
    /// 3V lithium coin cell (CR1632 for Contact V1, CR2450 for Motion V1,
    /// and similar CR cells for Leak V2, Climate V2).
    /// Flat voltage plateau around 2.8–2.9V, steep dropoff below ~2.7V.
    /// Raw byte range: 0–100.
    Lithium3VCoinCell,

    /// Single 1.5V AAA alkaline (Contact/Switch V2).
    /// More gradual discharge slope than lithium coin cells.
    /// Raw byte range: 0–~50 (half-scale since 1.5V is half of 3V reference).
    Alkaline1V5SingleAAA,

    /// Dual 1.5V AAA in series = 3V total (Motion V2).
    /// Alkaline discharge characteristics at the 3V scale.
    /// Raw byte range: 0–100.
    Alkaline3VDualAAA,
}

impl BatteryChemistry {
    /// Returns the appropriate battery chemistry for a given sensor type,
    /// or `None` for mains-powered or unknown devices.
    pub fn for_sensor(sensor_type: SensorType) -> Option<BatteryChemistry> {
        match sensor_type {
            // V1 sensors + Leak/Climate V2: CR-series 3V lithium coin cells
            SensorType::ContactV1 | SensorType::MotionV1 |
            SensorType::LeakV2 | SensorType::ClimateV2 => {
                Some(BatteryChemistry::Lithium3VCoinCell)
            }
            // Contact/Switch V2: single 1.5V AAA
            SensorType::ContactV2 => Some(BatteryChemistry::Alkaline1V5SingleAAA),
            // Motion V2: 2× AAA = 3V
            SensorType::MotionV2 => Some(BatteryChemistry::Alkaline3VDualAAA),
            // Chime is mains-powered; Unknown we can't map
            SensorType::Chime | SensorType::Unknown(_) => None,
        }
    }
}

/// Discharge curve lookup tables: (raw_byte, capacity_percent).
/// Points are in descending order of raw value.
///
/// Hardware conversion (from firmware RE, §6.4):
///   raw = AON_BATMON:BAT >> 3
///   voltage = raw / 32.0
///   raw = round(voltage × 32)

// 3V lithium coin cell: very flat plateau, then steep cliff
const LITHIUM_3V_CURVE: &[(u8, u8)] = &[
    (99, 100),   // 3.09V — fresh (3.0V nominal = 96)
    (93,  80),   // 2.91V — still on plateau
    (90,  50),   // 2.81V — plateau ending
    (86,  20),   // 2.69V — dropping fast
    (83,  10),   // 2.59V — very low
    (80,   5),   // 2.50V — critical
    (67,   0),   // 2.09V — dead
];

// Single AAA (1.5V): reports at half-scale, gradual slope
const ALKALINE_1V5_CURVE: &[(u8, u8)] = &[
    (50, 100),   // 1.56V — fresh
    (45,  80),   // 1.41V
    (42,  50),   // 1.31V
    (38,  25),   // 1.19V
    (35,  10),   // 1.09V
    (32,   0),   // 1.00V — dead
];

// Dual AAA (3V total): alkaline but at 3V scale
const ALKALINE_3V_CURVE: &[(u8, u8)] = &[
    (99, 100),   // 3.09V — fresh
    (90,  80),   // 2.81V
    (83,  50),   // 2.59V
    (77,  25),   // 2.41V
    (70,  10),   // 2.19V
    (64,   0),   // 2.00V — dead
];

/// Convert a raw voltage-proportional battery byte to estimated remaining capacity.
///
/// Uses piecewise linear interpolation on the discharge curve for the given chemistry.
/// Returns 0–100.
pub fn raw_to_capacity(raw: u8, chemistry: BatteryChemistry) -> u8 {
    let curve = match chemistry {
        BatteryChemistry::Lithium3VCoinCell => LITHIUM_3V_CURVE,
        BatteryChemistry::Alkaline1V5SingleAAA => ALKALINE_1V5_CURVE,
        BatteryChemistry::Alkaline3VDualAAA => ALKALINE_3V_CURVE,
    };
    interpolate(raw, curve)
}

/// Piecewise linear interpolation. Curve is in descending raw-value order.
fn interpolate(raw: u8, curve: &[(u8, u8)]) -> u8 {
    // Above the highest raw: clamp to max capacity
    if raw >= curve[0].0 {
        return curve[0].1;
    }
    // Below the lowest raw: clamp to min capacity
    let last = curve[curve.len() - 1];
    if raw <= last.0 {
        return last.1;
    }
    // Find bracketing segment and lerp
    for w in curve.windows(2) {
        let (raw_hi, cap_hi) = w[0];
        let (raw_lo, cap_lo) = w[1];
        if raw <= raw_hi && raw >= raw_lo {
            let frac = (raw - raw_lo) as f32 / (raw_hi - raw_lo) as f32;
            let cap = cap_lo as f32 + frac * (cap_hi as f32 - cap_lo as f32);
            return (cap.round() as u8).min(100);
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Coin cell curve ---

    #[test]
    fn coin_cell_full_battery() {
        assert_eq!(raw_to_capacity(99, BatteryChemistry::Lithium3VCoinCell), 100);
        // Above max raw still returns 100
        assert_eq!(raw_to_capacity(120, BatteryChemistry::Lithium3VCoinCell), 100);
    }

    #[test]
    fn coin_cell_dead_battery() {
        assert_eq!(raw_to_capacity(67, BatteryChemistry::Lithium3VCoinCell), 0);
        assert_eq!(raw_to_capacity(50, BatteryChemistry::Lithium3VCoinCell), 0);
    }

    #[test]
    fn coin_cell_plateau_stays_high() {
        // On the plateau (raw 93–99), capacity should be high
        assert!(raw_to_capacity(95, BatteryChemistry::Lithium3VCoinCell) >= 80);
    }

    #[test]
    fn coin_cell_drops_steeply() {
        // raw 86 should be much lower than raw 90 — steep cliff
        let cap_90 = raw_to_capacity(90, BatteryChemistry::Lithium3VCoinCell);
        let cap_86 = raw_to_capacity(86, BatteryChemistry::Lithium3VCoinCell);
        assert_eq!(cap_90, 50);
        assert_eq!(cap_86, 20);
    }

    #[test]
    fn coin_cell_interpolation() {
        // raw 91 is between (93,80) and (90,50) → should be ~60
        let cap = raw_to_capacity(91, BatteryChemistry::Lithium3VCoinCell);
        assert!(cap >= 55 && cap <= 65, "raw=91 → expected ~60, got {}", cap);
    }

    // --- Single AAA curve ---

    #[test]
    fn single_aaa_full() {
        assert_eq!(raw_to_capacity(50, BatteryChemistry::Alkaline1V5SingleAAA), 100);
    }

    #[test]
    fn single_aaa_dead() {
        assert_eq!(raw_to_capacity(32, BatteryChemistry::Alkaline1V5SingleAAA), 0);
    }

    #[test]
    fn single_aaa_midpoint() {
        assert_eq!(raw_to_capacity(42, BatteryChemistry::Alkaline1V5SingleAAA), 50);
    }

    // --- Dual AAA curve ---

    #[test]
    fn dual_aaa_full() {
        assert_eq!(raw_to_capacity(99, BatteryChemistry::Alkaline3VDualAAA), 100);
    }

    #[test]
    fn dual_aaa_dead() {
        assert_eq!(raw_to_capacity(64, BatteryChemistry::Alkaline3VDualAAA), 0);
    }

    // --- Chemistry mapping ---

    #[test]
    fn chemistry_mapping() {
        assert_eq!(BatteryChemistry::for_sensor(SensorType::ContactV1),
                   Some(BatteryChemistry::Lithium3VCoinCell));
        assert_eq!(BatteryChemistry::for_sensor(SensorType::MotionV1),
                   Some(BatteryChemistry::Lithium3VCoinCell));
        assert_eq!(BatteryChemistry::for_sensor(SensorType::LeakV2),
                   Some(BatteryChemistry::Lithium3VCoinCell));
        assert_eq!(BatteryChemistry::for_sensor(SensorType::ClimateV2),
                   Some(BatteryChemistry::Lithium3VCoinCell));
        assert_eq!(BatteryChemistry::for_sensor(SensorType::ContactV2),
                   Some(BatteryChemistry::Alkaline1V5SingleAAA));
        assert_eq!(BatteryChemistry::for_sensor(SensorType::MotionV2),
                   Some(BatteryChemistry::Alkaline3VDualAAA));
        assert_eq!(BatteryChemistry::for_sensor(SensorType::Chime), None);
        assert_eq!(BatteryChemistry::for_sensor(SensorType::Unknown(0xFF)), None);
    }
}
