
CREATE TABLE cryptobirds.contests_historical_rank (
`id` int NOT NULL PRIMARY KEY AUTO_INCREMENT,
`id_contest` int NOT NULL,
`id_user` int NOT NULL,
`contest_base_points` decimal(10,2) NOT NULL DEFAULT 0.00,
`bonus` decimal(10,2) NOT NULL  DEFAULT 0.00,
`contest_total_points` decimal(10, 2) NOT NULL DEFAULT 0.00,
`user_cumulated_points` decimal(10, 2),
`contests_by_user_count` int,
`is_contest_participant` varchar(1),
`top_counter` int,
`historic_rank` int,
`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
`updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP);

ALTER TABLE cryptobirds.contests_historical_rank 
ADD UNIQUE INDEX `UK_contest_historical_rank_contest_user` (`id_contest`, `id_user`) VISIBLE;

