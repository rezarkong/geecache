CREATE DATABASE IF NOT EXISTS `geecache`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `geecache`;

CREATE TABLE IF NOT EXISTS `scores` (
  `name` VARCHAR(64) NOT NULL,
  `score` VARCHAR(32) NOT NULL,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `scores` (`name`, `score`) VALUES
  ('Tom', '630'),
  ('Jack', '589'),
  ('Sam', '567')
ON DUPLICATE KEY UPDATE
  `score` = VALUES(`score`);
