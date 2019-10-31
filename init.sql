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

-- Keys

ALTER TABLE `node_types`
  ADD KEY `NODETYPES_NAME` (`name`);

ALTER TABLE `edge_types`
  ADD KEY `EDGETYPES_NAME` (`name`);

ALTER TABLE `nodes`
  ADD KEY `NODES_NAME`   (`name`),
  ADD KEY `NODES_WEIGHT` (`weight`);

ALTER TABLE `edges`
  ADD KEY `EDGES_WEIGHT` (`weight`),
  ADD KEY `EDGES_SDT`    (`source`, `destination`, `type`),
  ADD KEY `EDGES_TS`     (`type`, `source`),
  ADD KEY `EDGES_DT`     (`destination`, `type`);

-- Foreign keys

ALTER TABLE `nodes`
  ADD CONSTRAINT `NODE_TYPE_FK` FOREIGN KEY (`type`) REFERENCES `node_types` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE;

ALTER TABLE `edges`
  ADD CONSTRAINT `EDGES_SOURCE_FK` FOREIGN KEY (`source`) REFERENCES `nodes` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  ADD CONSTRAINT `EDGES_DESTINATION_FK` FOREIGN KEY (`destination`) REFERENCES `nodes` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  ADD CONSTRAINT `EDGES_TYPE_FK` FOREIGN KEY (`type`) REFERENCES `edge_types` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE;

