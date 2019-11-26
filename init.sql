-- Tables

CREATE TABLE IF NOT EXISTS `node_types` (
  `id`   INT(11) PRIMARY KEY AUTO_INCREMENT COMMENT 'Node type id',
  `name` VARCHAR(64)         DEFAULT NULL   COMMENT 'Node type name',
  `info` VARCHAR(256)        DEFAULT NULL   COMMENT 'Node type info'
) ENGINE=InnoDB                             COMMENT='Node types';

CREATE TABLE IF NOT EXISTS `edge_types` (
  `id`           INT(11) PRIMARY KEY AUTO_INCREMENT COMMENT 'Edge type id',
  `name`         VARCHAR(64)         DEFAULT NULL   COMMENT 'Edge type name',
  `extendedName` VARCHAR(128)        DEFAULT NULL   COMMENT 'Edge type extended',
  `info`         VARCHAR(256)        DEFAULT NULL   COMMENT 'Edge type info'
) ENGINE=InnoDB                                     COMMENT='Edge types';

CREATE TABLE IF NOT EXISTS `nodes` (
  `id`     INT(11)      PRIMARY KEY AUTO_INCREMENT COMMENT 'Node id',
  `name`   VARCHAR(256) NOT NULL                   COMMENT 'Node name',
  `type`   INT(11)      NOT NULL                   COMMENT 'Node type',
  `weight` INT(11)      NOT NULL                   COMMENT 'Node weight'
) ENGINE=InnoDB  DEFAULT COLLATE utf8_bin                                  COMMENT='Node types';

CREATE TABLE IF NOT EXISTS `edges` (
  `id`          INT(11) PRIMARY KEY AUTO_INCREMENT COMMENT 'Edge id',
  `source`      INT(11) NOT NULL                   COMMENT 'Edge source',
  `destination` INT(11) NOT NULL                   COMMENT 'Edge destination',
  `type`        INT(11) NOT NULL                   COMMENT 'Edge type',
  `weight`      INT(11) NOT NULL                   COMMENT 'Edge weight'
) ENGINE=InnoDB                                    COMMENT='Edges';

