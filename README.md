Weather Integrator
=========
[![Travis CI](https://travis-ci.org/nkoutroumanis/Weather-Integrator.svg?branch=master)](https://travis-ci.org/nkoutroumanis/Weather-Integrator)<br/>

Data integrator implementation in java for integrating weather information to spatio-temporal points.

This work provides a mechanism for enriching spatio-temporal point records (records that contain longitude, latitude and date information) with weather data;
attributes that describe characteristics of the prevailing weather conditions, such as temperature. The weather data can be found in GRIB files that store the values 
of weather attributes in serialized form and refer to a specific time period and to a specific geographical area (or the whole globe). 

GRIB is a standardized binary data format by the World Meteorological Organization's Commission for Basic Systems, commonly used in meteorology to store historical and forecast weather data.
Publicly GRIB files with forecast weather data are offered by [NoAA](https://www.ncdc.noaa.gov/data-access/model-data/model-datasets/global-forcast-system-gfs) and [ECMWF](https://apps.ecmwf.int/datasets/).
Other organizations may also distribute for free weather data sources in the form of GRIB files.

When choosing the GRIB files as the weather data source for the enrichment procedure, it is preferable to opt for those with the lowest grid scale for better accuracy.
Also, since 4 models are run per day each one starting at 00:00, 06:00, 12:00 and 18:00, prefer the files for each model with steps 000 or 003 or 006.

The step number of a grib file declares the hours ahead from the starting time of the model. The contained weather information is referring to that time ahead. 
GRIB files with steps > 000, contain both instantaneous weather attributes and average variables for some passed time. For instance, a GRIB file with step 006 
contain weather attributes that refer to 6-hours ahead from the starting time of the model and 6-hour average attributes that refer to the passed time period. 
 
 
 
Available Features that can be Integrated in Files 
------------                                       
 
integrating weather data in files (such as csv) consisting of records that contain information about Absolute Time and Coordinates (longitude and latitude) of a location.

Weather data is exported from [Grib files](http://weather.mailasail.com/Franks-Weather/Grib-Files-Explained)
(in a few words, a grib file contains weather data - features about weather for a certain time in a serialized form), and then is integrated in the records of files (csv, txt etc.). Grib files can be found [here](ftp://nomads.ncdc.noaa.gov/GFS/Grid4/)

A file can be enriched with weather data only if its records contain information about Absolute Time (Date & Time) and Coordinates (longitude and latitude).



Available Features that can be Integrated in Files
------------
The available features that can be exported from the Grib Files (and then be added on files) are the following;
* LatLon_Projection
* lat
* lon
* reftime
* time
* time_bounds
* time1
* isobaric
* height_above_ground
* height_above_ground_layer
* height_above_ground_layer_bounds
* pressure_difference_layer
* pressure_difference_layer_bounds
* height_above_ground1
* height_above_ground2
* sigma
* isobaric1
* isobaric2
* pressure_difference_layer1
* pressure_difference_layer1_bounds
* sigma_layer
* sigma_layer_bounds
* height_above_ground_layer1
* height_above_ground_layer1_bounds
* height_above_ground3
* altitude_above_msl
* potential_vorticity_surface
* height_above_ground4
* isobaric3
* pressure_difference_layer2
* pressure_difference_layer2_bounds
* depth_below_surface_layer
* depth_below_surface_layer_bounds
* Absolute_vorticity_isobaric
* Albedo_surface_3_Hour_Average
* Apparent_temperature_height_above_ground
* Cloud_mixing_ratio_isobaric
* Cloud_water_entire_atmosphere_single_layer
* Convective_available_potential_energy_surface
* Convective_available_potential_energy_pressure_difference_layer
* Convective_inhibition_surface
* Convective_inhibition_pressure_difference_layer
* Convective_precipitation_surface_3_Hour_Accumulation
* Dewpoint_temperature_height_above_ground
* Geopotential_height_potential_vorticity_surface
* Geopotential_height_surface
* Geopotential_height_isobaric
* Geopotential_height_zeroDegC_isotherm
* Geopotential_height_maximum_wind
* Geopotential_height_tropopause
* Geopotential_height_highest_tropospheric_freezing
* Haines_index_surface
* ICAO_Standard_Atmosphere_Reference_Height_maximum_wind
* ICAO_Standard_Atmosphere_Reference_Height_tropopause
* Ice_cover_surface
* Land_cover_0__sea_1__land_surface
* Latent_heat_net_flux_surface_3_Hour_Average
* Maximum_temperature_height_above_ground_3_Hour_Maximum
* Minimum_temperature_height_above_ground_3_Hour_Minimum
* Momentum_flux_u-component_surface_3_Hour_Average
* Momentum_flux_v-component_surface_3_Hour_Average
* Per_cent_frozen_precipitation_surface
* Potential_temperature_sigma
* Precipitable_water_entire_atmosphere_single_layer
* Precipitation_rate_surface_3_Hour_Average
* Pressure_low_cloud_bottom_3_Hour_Average
* Pressure_middle_cloud_bottom_3_Hour_Average
* Pressure_high_cloud_bottom_3_Hour_Average
* Pressure_low_cloud_top_3_Hour_Average
* Pressure_middle_cloud_top_3_Hour_Average
* Pressure_high_cloud_top_3_Hour_Average
* Pressure_potential_vorticity_surface
* Pressure_convective_cloud_bottom
* Pressure_convective_cloud_top
* Pressure_surface
* Pressure_maximum_wind
* Pressure_tropopause
* Pressure_height_above_ground
* Pressure_reduced_to_MSL_msl
* Relative_humidity_entire_atmosphere_single_layer
* Relative_humidity_highest_tropospheric_freezing
* Relative_humidity_pressure_difference_layer
* Relative_humidity_sigma_layer
* Relative_humidity_isobaric
* Relative_humidity_zeroDegC_isotherm
* Relative_humidity_height_above_ground
* Relative_humidity_sigma
* Sensible_heat_net_flux_surface_3_Hour_Average
* Snow_depth_surface
* Soil_temperature_depth_below_surface_layer
* Specific_humidity_pressure_difference_layer
* Specific_humidity_height_above_ground
* Storm_relative_helicity_height_above_ground_layer
* Temperature_low_cloud_top_3_Hour_Average
* Temperature_middle_cloud_top_3_Hour_Average
* Temperature_high_cloud_top_3_Hour_Average
* Temperature_potential_vorticity_surface
* Temperature_surface
* Temperature_pressure_difference_layer
* Temperature_isobaric
* Temperature_maximum_wind
* Temperature_altitude_above_msl
* Temperature_height_above_ground
* Temperature_tropopause
* Temperature_sigma
* Total_cloud_cover_low_cloud_3_Hour_Average
* Total_cloud_cover_middle_cloud_3_Hour_Average
* Total_cloud_cover_high_cloud_3_Hour_Average
* Total_cloud_cover_boundary_layer_cloud_3_Hour_Average
* Total_cloud_cover_entire_atmosphere_3_Hour_Average
* Total_cloud_cover_convective_cloud
* Total_ozone_entire_atmosphere_single_layer
* Total_precipitation_surface_3_Hour_Accumulation
* Categorical_Rain_surface_3_Hour_Average
* Categorical_Freezing_Rain_surface_3_Hour_Average
* Categorical_Ice_Pellets_surface_3_Hour_Average
* Categorical_Snow_surface_3_Hour_Average
* Convective_Precipitation_Rate_surface_3_Hour_Average
* Potential_Evaporation_Rate_surface
* Ozone_Mixing_Ratio_isobaric
* Vertical_Speed_Shear_tropopause
* Vertical_Speed_Shear_potential_vorticity_surface
* U-Component_Storm_Motion_height_above_ground_layer
* V-Component_Storm_Motion_height_above_ground_layer
* Ventilation_Rate_planetary_boundary
* MSLP_Eta_model_reduction_msl
* 5-Wave_Geopotential_Height_isobaric
* Zonal_Flux_of_Gravity_Wave_Stress_surface_3_Hour_Average
* Meridional_Flux_of_Gravity_Wave_Stress_surface_3_Hour_Average
* Planetary_Boundary_Layer_Height_surface
* Pressure_of_level_from_which_parcel_was_lifted_pressure_difference_layer
* Downward_Short-Wave_Radiation_Flux_surface_3_Hour_Average
* Upward_Short-Wave_Radiation_Flux_surface_3_Hour_Average
* Upward_Short-Wave_Radiation_Flux_atmosphere_top_3_Hour_Average
* Downward_Long-Wave_Radp_Flux_surface_3_Hour_Average
* Upward_Long-Wave_Radp_Flux_surface_3_Hour_Average
* Upward_Long-Wave_Radp_Flux_atmosphere_top_3_Hour_Average
* Cloud_Work_Function_entire_atmosphere_single_layer_3_Hour_Average
* Sunshine_Duration_surface
* Surface_Lifted_Index_surface
* Best_4_layer_Lifted_Index_surface
* Volumetric_Soil_Moisture_Content_depth_below_surface_layer
* Ground_Heat_Flux_surface_3_Hour_Average
* Wilting_Point_surface
* Field_Capacity_surface
* Vertical_velocity_pressure_isobaric
* Vertical_velocity_pressure_sigma
* Water_equivalent_of_accumulated_snow_depth_surface
* Water_runoff_surface_3_Hour_Accumulation
* Wind_speed_gust_surface
* u-component_of_wind_planetary_boundary
* u-component_of_wind_potential_vorticity_surface
* u-component_of_wind_pressure_difference_layer
* u-component_of_wind_isobaric
* u-component_of_wind_maximum_wind
* u-component_of_wind_altitude_above_msl
* u-component_of_wind_height_above_ground
* u-component_of_wind_tropopause
* u-component_of_wind_sigma
* v-component_of_wind_planetary_boundary
* v-component_of_wind_potential_vorticity_surface
* v-component_of_wind_pressure_difference_layer
* v-component_of_wind_isobaric
* v-component_of_wind_maximum_wind
* v-component_of_wind_altitude_above_msl
* v-component_of_wind_height_above_ground
* v-component_of_wind_tropopause
* v-component_of_wind_sigma

Features correlated with Precipitation
------------
The available features that are accociated with Precipitation are the following;
* Per_cent_frozen_precipitation_surface
* Precipitable_water_entire_atmosphere_single_layer
* Precipitation_rate_surface_3_Hour_Average
* Storm_relative_helicity_height_above_ground_layer
* Total_precipitation_surface_3_Hour_Accumulation
* Categorical_Rain_surface_3_Hour_Average
* Categorical_Freezing_Rain_surface_3_Hour_Average
* Categorical_Ice_Pellets_surface_3_Hour_Average
* Categorical_Snow_surface_3_Hour_Average
* Convective_Precipitation_Rate_surface_3_Hour_Average
* Convective_precipitation_surface_3_Hour_Accumulation
* U-Component_Storm_Motion_height_above_ground_layer
* V-Component_Storm_Motion_height_above_ground_layer