// Calculations related to atmospheric pressure products
// Define constants used in WMO sea-level pressure reduction (WMO-No. 8, Chapter 3, Section 3.7.2).
const G_N: f64 = 9.80665; // Standard acceleration of gravity [m s^-2]
const R: f64 = 287.05; // Gas constant of dry air [J kg^-1 K^-1]
const LAPSE_RATE: f64 = 0.0065; // Assumed lapse-rate in the fictitious air column [K gpm^-1]
const VAPOR_CORRECTION_COEFF: f64 = 0.12; // WMO empirical vapor pressure correction `C_h` [K/hPa]
const CELSIUS_TO_KELVIN: f64 = 273.15; // Offset between deg.C and K [K]

/// Calculates air pressure reduced to mean sea level `p_0` in [hPa].
///
/// ref.: Guide to Instruments and Methods of Observation (WMO-No. 8) - Part I, Chapter 3:
/// Measurement of Atmospheric Pressure - Section 3.7.2: General reduction formula
/// (feasible for sensors below 750 m; from WMO, 1964, p. 22, equation 2)
///
/// Equation (3.2): p_0 = p_s * exp( (g_n / R) * H_p / (T_mv) )
/// where mean virtual temperature of the fictitious air column `T_mv` is:
/// T_mv = t + 273.15 + (a * H_p / 2) + (0.12 * e)
///
/// p_0: air_pressure_at_sea_level [hPa]
/// p_s: surface_air_pressure at sensor/station level [hPa]
/// t: air_temperature at sensor/station level [deg.C] / WMO's T_s refers to t in Kelvin
/// e: water_vapor_partial_pressure [hPa] / WMO uses e_s: vapour pressure at the station [hPa]
/// H_p: sensor/station elevation above mean sea level [m]
///
/// [IMPORTANT] Elevation selection in Stinfosys (`sensor_elevation` is kind of a mislabel):
/// 1. `hp` from `station` table: Standard elevation of the station's pressure sensor (barometer) above MSL.
/// 2.  `lvl` (lard) / `hlevel` (obs_pgm): For maritime stations (i.e. offshore platforms) with multiple
///    sensors at different heights, SHOULD we use the sensor height/level even if a station-wide barometer
///    elevation `hp` is defined???
/// 3. `hs` from `station`` table: Fallback station elevation??? (defined by WMO as the elevation of
///    the precipitation gauge) if neither `hp` nor `hlevel` is available.
pub fn calculate_air_pressure_at_sea_level(
    surface_air_pressure: f64,
    air_temperature: f64,
    water_vapor_partial_pressure: f64,
    sensor_elevation: f64,
) -> f64 {
    let mean_virtual_temp_k = (air_temperature + CELSIUS_TO_KELVIN)
        + (LAPSE_RATE * sensor_elevation / 2.0)
        + (VAPOR_CORRECTION_COEFF * water_vapor_partial_pressure);

    let exponent = (G_N / R) * sensor_elevation / mean_virtual_temp_k;
    surface_air_pressure * exponent.exp()
}

/// Wrapper for air_pressure_at_sea_level `p_0` in [hPa] or `PR` - 178
/// from sensor observations: `PO` - 173, `TA` - 211, `UU` - 262, `lvl` - sensor level.
pub fn air_pressure_at_sea_level(
    surface_air_pressure: f64,
    air_temperature: f64,
    relative_humidity: f64,
    sensor_elevation: f64,
) -> f64 {
    let water_vapor_partial_pressure =
        crate::calculations::humidity::water_vapor_partial_pressure_in_air(
            air_temperature,
            relative_humidity,
        );
    calculate_air_pressure_at_sea_level(
        surface_air_pressure,
        air_temperature,
        water_vapor_partial_pressure,
        sensor_elevation,
    )
}
