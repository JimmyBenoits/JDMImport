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

