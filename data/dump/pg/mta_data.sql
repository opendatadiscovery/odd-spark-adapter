CREATE DATABASE mta_data;

CREATE TABLE IF NOT EXISTS mta_transform (
                               latitude double precision DEFAULT NULL,
                               longitude double precision DEFAULT NULL,
                               time_received text,
                               vehicle_id integer DEFAULT NULL,
                               distance_along_trip double precision DEFAULT NULL,
                               inferred_direction_id integer DEFAULT NULL,
                               inferred_phase text,
                               inferred_route_id text,
                               inferred_trip_id text,
                               next_scheduled_stop_distance double precision DEFAULT NULL,
                               next_scheduled_stop_id text,
                               report_hour text
);