// calculations related to humidity products
use crate::error::Error;

fn calculate_saturation_vapor_pressure(air_temperature: f64) -> f64 {
    // ref.: The Relationship between Relative Humidity and the Dewpoint Temperature in Moist Air:
    // A Simple Conversion and Applications (2005) - https://doi.org/10.1175/BAMS-86-2-225
    // Equation (6) : e_s = c * exp( (a * ta) / (b + ta) )
    // e_s = saturation_vapor_pressure [hPa]
    // ta = air_temperature [deg.C]
    // 'Commonly known as the Magnus formula (empirical) although a rather inaccurate attribution'
    //
    // extra info:  https://en.wikipedia.org/wiki/Vapor_pressure_of_water
    // Water vapor pressure behaves non-linearly with temperature:
    // - At higher temperatures, water molecules have more kinetic energy, leading to a rapid increase in vapor pressure.
    // - At lower temperatures (especially below freezing), the relationship changes due to differences in the behavior of ice versus liquid water.
    // Approxmations: Tetens or Aguste-Roche-Magnus equation... the form is the same, but coefficients differ based on temperature range.
    //
    // [IMPORTANT] Here we use the following coefficients:
    // for ta >  0, a = 17.08085 [rate] or [deg.C / deg.C], b = 234.175 [deg.C], and c = 6.10780 [hPa]. DEW POINT
    // for ta <= 0, a = 17.84362 [rate] or [deg.C / deg.C], b = 245.425 [deg.C], and c = 6.10780 [hPa]. FROST POINT

    if air_temperature <= 0.0 {
        return 6.10780 * (17.84362 * air_temperature / (245.425 + air_temperature)).exp();
    }
    6.10780 * (17.08085 * air_temperature / (234.175 + air_temperature)).exp()
}

pub fn calculate_water_vapor_partial_pressure(
    saturation_vapor_pressure: f64,
    relative_humidity: f64,
) -> f64 {
    // ref.: The Relationship between Relative Humidity and the Dewpoint Temperature in Moist Air:
    // A Simple Conversion and Applications (2005) - https://doi.org/10.1175/BAMS-86-2-225
    // Equation (3) : RH = 100 * e / e_s  -or-  e = (RH * e_s) / 100
    // 'RH' Relative humidity is commonly defined as the ratio of
    // 'e' the actual water vapor pressure [hPa] to
    // 'e_s' the 'saturation' vapor pressure [hPa]
    // 1 hPa = 1 mb = 100 Pascals are units commonly used in meteorology and atmospheric sciences.

    (relative_humidity * saturation_vapor_pressure) / 100.0
}

pub fn calculate_dew_point_temperature(
    water_vapor_partial_pressure: f64,
    air_temperature: f64,
) -> f64 {
    // ref.: The Relationship between Relative Humidity and the Dewpoint Temperature in Moist Air:
    // A Simple Conversion and Applications (2005) - https://doi.org/10.1175/BAMS-86-2-225
    // Equation (7) : td = ( b * ln( e / c ) ) / ( a - ln( e / c ) )
    // td = dew_point_temperature [deg.C]
    // e = water_vapor_partial_pressure [hPa]
    //
    // ekstra info: Guide to Instruments and Methods of Observation (WMO-No. 8)
    // Chapter 4: Humidity - Annex 4.B: Formulae for the computation of measures of humidity
    //
    // Should we set a condition that dew_point_temperature cannot be greater than air_temperature???

    if air_temperature <= 0.0 {
        return 245.425 * (water_vapor_partial_pressure / 6.10780).ln()
            / (17.84362 - (water_vapor_partial_pressure / 6.10780).ln());
    }
    234.175 * (water_vapor_partial_pressure / 6.10780).ln()
        / (17.08085 - (water_vapor_partial_pressure / 6.10780).ln())
}

pub fn calculate_humidity_mixing_ratio(
    water_vapor_partial_pressure: f64,
    surface_air_pressure: f64,
) -> f64 {
    // ref.: Guide to Instruments and Methods of Observation (WMO-No. 8) - Chapter 4: Humidity
    // Annex 4.A: Definitions and specifications of water vapor in the atmosphere
    // Equation (4.A.1) : r = m_v / m_a
    // mixing ratio 'r' is defined as the mass 'm_v' of water vapor per unit mass 'm_a' of dry air in g/kg or kg/kg
    //
    // Here, the mixing ratio is DERIVED FROM Equation (4.A.6) : e = po * r / ( epsilon + r ), then giving:
    // Equation: r = epsilon * ( e / (po - e) )
    // r: humidity_mixing_ratio [kg/kg], - see conversion note below
    // e: water_vapor_partial_pressure [hPa],
    // po: surface_air_pressure [hPa],
    // epsilon: ratio of the molecular weight of water vapor to dry air, approximately 0.62198 [dimensionless] or [g/mol / g/mol].
    //
    // IMPORTANT!! Add a CONVERSION from [kg/kg] to [g/kg] by multiplying by 1000 to match units for 'r' in stinfosys i.e. [g/kg].

    1000.0 * 0.62198 * water_vapor_partial_pressure
        / (surface_air_pressure - water_vapor_partial_pressure)
}

pub fn calculate_specific_humidity(humidity_mixing_ratio: f64) -> f64 {
    // ref.: Guide to Instruments and Methods of Observation (WMO-No. 8) - Chapter 4: Humidity
    // Annex 4.A: Definitions and specifications of water vapor in the atmosphere
    // Equation (4.A.2) : q = m_v / ( m_a + m_v )
    // specific humidity 'q' is defined as the mass 'm_v' of water vapor per unit mass of moist air in g/kg or kg/kg
    //
    // Here, we subsitute 'm_v' with the mixing ratio 'r' from Equation (4.A.1) : m_v = r * m_a in Equation (4.A.2), then giving:
    // Equation: q = r / (1 + r) - !!NOT USED AS SUCH HERE!!
    // 'r' the actual water vapor dry mass mixing ratio [kg/kg] - see conversion note below
    // 'q' specific humidity [kg/kg] - see conversion note below
    //
    // IMPORTANT!! Here, the function is adapted for [g/kg] units to match units for 'r' and 'q' in stinfosys i.e. [g/kg].
    // Equation: q = (r / 1000) / (1 + (r / 1000)), giving
    // FINAL Equation: q = r / (1000 + r)
    // 'r' the actual water vapor dry mass mixing ratio [g/kg]
    // 'q' specific humidity [g/kg]

    humidity_mixing_ratio / (1000.0 + humidity_mixing_ratio)
}

// mean(water_vapor_partial_pressure_in_air P1D)
pub fn water_vapor_partial_pressure_in_air(
    air_temperature: f64,
    relative_humidity: f64,
) -> Result<f64, Error> {
    let saturation_vapor_pressure = calculate_saturation_vapor_pressure(air_temperature);
    Ok(calculate_water_vapor_partial_pressure(
        saturation_vapor_pressure,
        relative_humidity,
    ))
}

// dew_point_temperature
pub fn dew_point_temperature(air_temperature: f64, relative_humidity: f64) -> Result<f64, Error> {
    let saturation_vapor_pressure = calculate_saturation_vapor_pressure(air_temperature);
    let water_vapor_partial_pressure =
        calculate_water_vapor_partial_pressure(saturation_vapor_pressure, relative_humidity);
    Ok(calculate_dew_point_temperature(
        water_vapor_partial_pressure,
        air_temperature,
    ))
}

// over_time(humidity_mixing_ratio P1D)
pub fn humidity_mixing_ratio(
    air_temperature: f64,
    relative_humidity: f64,
    surface_air_pressure: f64,
) -> Result<f64, Error> {
    let saturation_vapor_pressure = calculate_saturation_vapor_pressure(air_temperature);
    let water_vapor_partial_pressure =
        calculate_water_vapor_partial_pressure(saturation_vapor_pressure, relative_humidity);
    Ok(calculate_humidity_mixing_ratio(
        water_vapor_partial_pressure,
        surface_air_pressure,
    ))
}

// specific_humidity
pub fn specific_humidity(
    air_temperature: f64,
    relative_humidity: f64,
    surface_air_pressure: f64,
) -> Result<f64, Error> {
    let saturation_vapor_pressure = calculate_saturation_vapor_pressure(air_temperature);
    let water_vapor_partial_pressure =
        calculate_water_vapor_partial_pressure(saturation_vapor_pressure, relative_humidity);
    let humidity_mixing_ratio =
        calculate_humidity_mixing_ratio(water_vapor_partial_pressure, surface_air_pressure);
    Ok(calculate_specific_humidity(humidity_mixing_ratio))
}
