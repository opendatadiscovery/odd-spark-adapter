-- Adminer 4.8.1 MySQL 5.7.29 dump

SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

SET NAMES utf8mb4;

CREATE DATABASE `mta_data` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci */;
USE `mta_data`;

DROP TABLE IF EXISTS `mta_reports`;
CREATE TABLE `mta_reports` (
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `time_received` text COLLATE utf8mb4_unicode_520_ci,
  `vehicle_id` int(11) DEFAULT NULL,
  `distance_along_trip` double DEFAULT NULL,
  `inferred_direction_id` int(11) DEFAULT NULL,
  `inferred_phase` text COLLATE utf8mb4_unicode_520_ci,
  `inferred_route_id` text COLLATE utf8mb4_unicode_520_ci,
  `inferred_trip_id` text COLLATE utf8mb4_unicode_520_ci,
  `next_scheduled_stop_distance` double DEFAULT NULL,
  `next_scheduled_stop_id` text COLLATE utf8mb4_unicode_520_ci,
  `report_hour` text COLLATE utf8mb4_unicode_520_ci,
  `report_date` text COLLATE utf8mb4_unicode_520_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO `mta_reports` (`latitude`, `longitude`, `time_received`, `vehicle_id`, `distance_along_trip`, `inferred_direction_id`, `inferred_phase`, `inferred_route_id`, `inferred_trip_id`, `next_scheduled_stop_distance`, `next_scheduled_stop_id`, `report_hour`, `report_date`) VALUES
(40.668602,	-73.986697,	'2014-08-01 04:00:00',	470,	4135.34710710144,	1,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:02',	471,	4135.34710710144,	2,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:04',	472,	4135.34710710144,	3,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:14',	473,	4135.34710710144,	4,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:20',	474,	4135.34710710144,	5,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:21',	475,	4135.34710710144,	6,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:31',	476,	4135.34710710144,	7,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:41',	477,	4135.34710710144,	8,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:51',	478,	4135.34710710144,	9,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	'2014-08-01 04:00:00',	'2014-08-01'),
(40.668602,	-73.986697,	'2014-08-01 04:00:61',	479,	4135.34710710144,	10,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	NULL,	NULL),
(40.668602,	-73.986697,	'2014-08-01 04:00:71',	480,	4135.34710710144,	11,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	NULL,	NULL),
(40.668602,	-73.986697,	'2014-08-01 04:00:81',	481,	4135.34710710144,	12,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	NULL,	NULL),
(40.668602,	-73.986697,	'2014-08-01 04:00:91',	482,	4135.34710710144,	14,	'IN_PROGRESS',	'MTA NYCT_B63',	'MTA NYCT_JG_C4-Weekday-141500_B63_123',	2.63183804205619,	'MTA_305423',	NULL,	NULL);

DROP TABLE IF EXISTS `vehicle`;
CREATE TABLE `vehicle`
(
    `id`   int(11) NOT NULL,
    `name` varchar(100) COLLATE utf8mb4_unicode_520_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;

INSERT INTO `vehicle` (`id`, `name`)
VALUES (470, 'bus'),
       (471, 'train'),
       (472, 'airplain'),
       (473, 'car'),
       (474, 'name'),
       (475, 'name'),
       (476, 'name'),
       (477, 'name'),
       (478, 'name'),
       (479, 'name'),
       (480, 'name'),
       (481, 'name'),
       (482, 'name');

-- 2021-11-28 15:23:48
