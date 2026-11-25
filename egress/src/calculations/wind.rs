// Calculations related to wind products
// Define constants used in the NORSOK wind profile calculation [Standards Norway, 2007].
const NORSOK_C_COEFF: f64 = 5.73e-2; // [dimensionless]
const NORSOK_U_COEFF: f64 = 0.15; // [s/m]

/// Calculates the wind speed `u(z)` at height `z` in [m/s] using the NORSOK wind profile.
///
/// ref.: Standards Norway (2007) - NORSOK Standard N-003: Actions and action effects
/// Section 2.5: NORSOK Wind Profile (developed from measurements at the island Frøya in Norway)
/// Equation (10) : u(z) = u_zr * [ 1 + C * ln( z / z_r ) ]
/// where C = 5.73 * 10^-2 * [ 1 + 0.15 * u_zr ]^(1/2)
///
/// u(z): wind_speed at target height `z` [m/s]
/// u_zr: wind_speed at reference height `z_r` [m/s]
/// z: target_height above sea level [m]
/// z_r: reference_height above sea level, in our case level [m]
/// C: dimensionless surface drag / turbulence parameter dependent on `u_zr`
///
/// [IMPORTANT] This empirical profile depends only on wind speed and height (assumes neutral atmospheric stability).
/// Note that standard NORSOK N-003 defines `u_zr` as the 1-hour mean wind speed at `z_r = 10.0` [m].
pub fn calculate_norsok_wind_profile(
    wind_speed: f64,
    target_height: f64,
    reference_height: f64,
) -> f64 {
    let c = NORSOK_C_COEFF * (1.0 + NORSOK_U_COEFF * reference_wind_speed).sqrt();
    wind_speed * (1.0 + c * (target_height / reference_height).ln())
}
