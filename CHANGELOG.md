# Changelog

## [0.11.0](https://github.com/intigraph/chaski/compare/v0.10.0...v0.11.0) (2026-11-28)


### Features

* **emitter:** reviewed expiry of stale attempted/bound scheduled firings (aegis-se8) ([3de7655](https://github.com/intigraph/chaski/commit/3de76551e199ea0e216e9ac5e15a60f2c8fd8a16))
* **emitter:** reviewed expiry of stale attempted/bound scheduled firings (aegis-se8) ([c03ae80](https://github.com/intigraph/chaski/commit/c03ae809663e11a62c504bcabafbeacdbfa7f703))
* **github-notify:** push outside public-repo activity to ntfy once; crew/bot to digest (aegis-za4f.7) ([#95](https://github.com/intigraph/chaski/issues/95)) ([f8b6d74](https://github.com/intigraph/chaski/commit/f8b6d74965f14834e24eb69fda528c47f81e1668))
* **github-watch:** one draft job per unanswered outside thread (aegis-za4f.7) ([#103](https://github.com/intigraph/chaski/issues/103)) ([eaf53ed](https://github.com/intigraph/chaski/commit/eaf53ed3e3292a685a5ec12b40e81ff163915f92))
* **github-work:** record event-to-spawn latency on dispatch; test the real st spawner (aegis-dja) ([e018096](https://github.com/intigraph/chaski/commit/e018096c3af11d61314cd9abfa25034d51555a02))
* **github-work:** record event-to-spawn latency; test the real st spawner (aegis-dja) ([f87f7f4](https://github.com/intigraph/chaski/commit/f87f7f47a8e00ffbe9e2ec0cecf48b4a44c9b389))
* **main-red:** recruit one triage task per red default-branch sha; remove the stub spawner (aegis-dja) ([#101](https://github.com/intigraph/chaski/issues/101)) ([769665d](https://github.com/intigraph/chaski/commit/769665d290fffcbd8bcf3bacfa32955a5eef6eb1))
* **pr-lifecycle:** recruit review/fix/merge jobs from PR state (aegis-za4f.21) ([170e835](https://github.com/intigraph/chaski/commit/170e8358eddd0b76eedb126e10c4d878fe971ce5))
* **pr-lifecycle:** recruit review/fix/merge jobs from PR state (aegis-za4f.21) ([b6c2aac](https://github.com/intigraph/chaski/commit/b6c2aacc48835349ae5d85cf7109111d97969ca3))


### Bug Fixes

* **budget:** cap reservations at the attainable bucket; emitter starvation guard; count NotAdmitted (aegis-exe6.1) ([#104](https://github.com/intigraph/chaski/issues/104)) ([2d91113](https://github.com/intigraph/chaski/commit/2d91113ca848ad60af02e2acc3b14df02f1082c9))
* **budget:** reserve a measured one-item cost and wait; a transient failure can not strand a kind (aegis-exe6.2) ([#106](https://github.com/intigraph/chaski/issues/106)) ([683c506](https://github.com/intigraph/chaski/commit/683c5063b12de1b359dd96dc83cd1e2d754bd7b6))
* **main-red:** red_at is when the failed check completed, not when it started ([#102](https://github.com/intigraph/chaski/issues/102)) ([af7d57e](https://github.com/intigraph/chaski/commit/af7d57e5f7fec5864e2f2158beaf5fd5f4eb344c))
* **packaging:** ship reconcile.py, and fail the wheel smoke on any unshipped local import ([0b2da59](https://github.com/intigraph/chaski/commit/0b2da59108d03ec1bd6fb41ca4b9a92a99f4d2d5))
* **packaging:** ship reconcile.py, and fail the wheel smoke on any unshipped local import (aegis-d2q0) ([938f84f](https://github.com/intigraph/chaski/commit/938f84f651ded29d45e794558d7159a9af63dbd7))
* **pr-lifecycle:** a main sync keeps the verdict; no re-review when main moves (aegis-za4f.21) ([#108](https://github.com/intigraph/chaski/issues/108)) ([8837fb6](https://github.com/intigraph/chaski/commit/8837fb676857f2a03f63030ccfd462a279ef96ba))
* **pr-lifecycle:** add --version, report truncated PR lists and stalled PRs (aegis-za4f.21) ([fa3a023](https://github.com/intigraph/chaski/commit/fa3a0231f262d2b452af5d9644a80dd0ac242a2e))
* **pr-lifecycle:** only trusted GitHub input recruits jobs (aegis-za4f.21) ([23416f8](https://github.com/intigraph/chaski/commit/23416f819f6f448580a9cf651d7b038f7cc5fc2c))
* **scheduled-expiry:** apply takes custody for a stream-mode consumer without a cursor file (aegis-se8) ([b69ecee](https://github.com/intigraph/chaski/commit/b69eceea90ae5746dd40b8cf71825bf431d64545))
* **scheduled-expiry:** apply takes custody for a stream-mode consumer without a cursor file (aegis-se8) ([2e3925b](https://github.com/intigraph/chaski/commit/2e3925b01c345090acf2e6c6aef679d3cd0c1336))
* **scheduled:** a delivery TTL keeps stale retries from starving the newest slot (aegis-se8) ([1c302e7](https://github.com/intigraph/chaski/commit/1c302e7956557b91079f1481474dd3322f6a2c91))
* **scheduled:** a delivery TTL keeps stale retries from starving the newest slot (aegis-se8) ([59bc168](https://github.com/intigraph/chaski/commit/59bc168ab1cbf375c1bf88c0ba91dd6f9fc947bb))
* **share:** surface the failing tool's stderr when a publish command fails ([#90](https://github.com/intigraph/chaski/issues/90)) ([692eb0c](https://github.com/intigraph/chaski/commit/692eb0cbf1f5eae739bdac8ce8a0e1100771048d))


### Reverts

* count route calls in the bounded reconcile share ([#89](https://github.com/intigraph/chaski/issues/89)) ([6ec9059](https://github.com/intigraph/chaski/commit/6ec905956723f3d16034e0e87cff08a58750e87a))
* count route calls in the bounded reconcile share ([#89](https://github.com/intigraph/chaski/issues/89)) ([e06ffa9](https://github.com/intigraph/chaski/commit/e06ffa998cc243ac53b3c8f2749d789a4653aef3))


### Documentation

* point the stack table at the intigraph organization ([#107](https://github.com/intigraph/chaski/issues/107)) ([201c965](https://github.com/intigraph/chaski/commit/201c9656b00d67a78e40d9a4916d9fc7bb5f1a90))

## [0.10.0](https://github.com/intigraph/chaski/compare/v0.9.0...v0.10.0) (2026-11-26)


### Features

* **reconcile:** expose discovery progress per emitter (aegis-p1u4) ([172220a](https://github.com/intigraph/chaski/commit/172220a752da26aed8c13b9fb28e347582e05363))
* **reconcile:** expose discovery progress per emitter (aegis-p1u4) ([38075d2](https://github.com/intigraph/chaski/commit/38075d29c121a402b0842c5087534072702c1594))


### Bug Fixes

* **incremental:** count route calls in the bounded reconcile share ([8694b95](https://github.com/intigraph/chaski/commit/8694b95658130a86ca7372d19fcf651d418f37da))
* **incremental:** count route calls in the bounded reconcile share (aegis-g68w.14) ([65a92df](https://github.com/intigraph/chaski/commit/65a92df28a1ed528f4243e936f105d220a3b4435))
* **incremental:** reserve by measured cost and give reconcile work a bounded share (aegis-g68w.14) ([4f4b86a](https://github.com/intigraph/chaski/commit/4f4b86af736b45239eda52f5e1d24106d7748dbc))
* **incremental:** reserve by measured cost and give reconcile work a bounded share (aegis-g68w.14) ([624b7e5](https://github.com/intigraph/chaski/commit/624b7e5b38852922c30d62b74790cfcc907f00bc))

## [0.9.0](https://github.com/intigraph/chaski/compare/v0.8.0...v0.9.0) (2026-11-25)


### Features

* **emitter:** one-time reviewed recovery scope migration (aegis-ssvr) ([73023bd](https://github.com/intigraph/chaski/commit/73023bdb57ef9d8070211592695ee2a01eb6c9bc))
* **emitter:** one-time reviewed recovery scope migration (aegis-ssvr) ([25b32f1](https://github.com/intigraph/chaski/commit/25b32f12f0e87f65ec101a07128397d97b6ed16b))
* **github-routes:** opt-in release-assets kind, one judgment per release ([#81](https://github.com/intigraph/chaski/issues/81)) ([571a179](https://github.com/intigraph/chaski/commit/571a1796d81ba2a166a5fafdb36362abd8b78fe1))


### Bug Fixes

* **emitter:** durable migration log opened before the transaction ([#85](https://github.com/intigraph/chaski/issues/85) review) ([571ca42](https://github.com/intigraph/chaski/commit/571ca42f102e045bb47b4c28d40fda5f2df3ce32))
* **github-routes:** refuse a release-assets backstop at or past the window ([#83](https://github.com/intigraph/chaski/issues/83)) ([a8ed818](https://github.com/intigraph/chaski/commit/a8ed818656bde64da05480956dd100d6e20b24da))
* **metrics:** report change lag as observed watermark minus cursor, never -1 ([4475e5a](https://github.com/intigraph/chaski/commit/4475e5ad95ff5332b2ce304f8565eb876a00509b))
* **metrics:** report change lag as observed watermark minus cursor, never -1 ([7fe8be8](https://github.com/intigraph/chaski/commit/7fe8be80f59e77e49d044efdf43a3f88be758f91))

## [0.8.0](https://github.com/intigraph/chaski/compare/v0.7.1...v0.8.0) (2026-11-22)


### Features

* **metrics:** expose adapter request cost and unknown accounting ([#78](https://github.com/intigraph/chaski/issues/78)) ([310ba13](https://github.com/intigraph/chaski/commit/310ba13445c61a6d53821f3ea4575a60b6e9651d))
* **outbox:** expire retained outbox firings with immutable receipts and delivery fences ([#72](https://github.com/intigraph/chaski/issues/72)) ([7a700bc](https://github.com/intigraph/chaski/commit/7a700bccd32a320cc88aa61bb4d0007dd3f0e0c9))
* **reconcile:** catch up after downtime without replay, under one request budget (C1) ([#77](https://github.com/intigraph/chaski/issues/77)) ([d238c52](https://github.com/intigraph/chaski/commit/d238c52e5810315cd74487b728f3bae756a77063))
* restrict GitHub routing to configured work kinds ([#80](https://github.com/intigraph/chaski/issues/80)) ([5d05834](https://github.com/intigraph/chaski/commit/5d058341b54a5c0da224610cdd9fd904346e9ac3))


### Bug Fixes

* **emitter:** bound the incremental drain and back off refused sink setup ([#76](https://github.com/intigraph/chaski/issues/76)) ([34390f9](https://github.com/intigraph/chaski/commit/34390f9c4a70919679c42b696d80a997f8ef8210))
* **graph:** select receiver-compatible writer vocabulary ([#79](https://github.com/intigraph/chaski/issues/79)) ([8673f3c](https://github.com/intigraph/chaski/commit/8673f3c840b5d00512785bc3622f45d40b74b644))

## [0.7.1](https://github.com/intigraph/chaski/compare/v0.7.0...v0.7.1) (2026-11-14)


### Bug Fixes

* use published vocabulary in graph writers and dual readers ([#71](https://github.com/intigraph/chaski/issues/71)) ([82cef2f](https://github.com/intigraph/chaski/commit/82cef2fe877d9aa7e3504382bf7a4cb0ed0b14a0))

## [0.7.0](https://github.com/intigraph/chaski/compare/v0.6.0...v0.7.0) (2026-11-13)


### Features

* **schedules:** project one declared UTC occurrence ([#73](https://github.com/intigraph/chaski/issues/73)) ([40d1122](https://github.com/intigraph/chaski/commit/40d112285c4d5fc092c301e82a40085723e6f78c))


### Bug Fixes

* **command-sink:** preserve bounded failure correlation records ([#69](https://github.com/intigraph/chaski/issues/69)) ([64e4845](https://github.com/intigraph/chaski/commit/64e484528d1b7f6955dbdff01e9d4e2c2a367464))

## [0.6.0](https://github.com/intigraph/chaski/compare/v0.5.0...v0.6.0) (2026-11-12)


### Features

* **github:** queue stale PR work for named crew ([#67](https://github.com/intigraph/chaski/issues/67)) ([1076f28](https://github.com/intigraph/chaski/commit/1076f28b3f0a8cc31b3fdf69556621633ab18a35))

## [0.5.0](https://github.com/intigraph/chaski/compare/v0.4.2...v0.5.0) (2026-11-09)


### Features

* **consumer:** add read-only source binding preflight ([#65](https://github.com/intigraph/chaski/issues/65)) ([1de946a](https://github.com/intigraph/chaski/commit/1de946ac7b2fa2873a010491a3af2bfe6f3bf4be))


### Bug Fixes

* **github:** normalize compact references before routing ([#63](https://github.com/intigraph/chaski/issues/63)) ([a65caf8](https://github.com/intigraph/chaski/commit/a65caf85b593818a05329df522817dab9362bb00))

## [0.4.2](https://github.com/intigraph/chaski/compare/v0.4.1...v0.4.2) (2026-11-06)


### Bug Fixes

* **cli:** expose read-only versions on helper commands ([#61](https://github.com/intigraph/chaski/issues/61)) ([5a091ef](https://github.com/intigraph/chaski/commit/5a091efc1b4d53acc6c8697b6b77e512265bec7e))

## [0.4.1](https://github.com/intigraph/chaski/compare/v0.4.0...v0.4.1) (2026-11-05)


### Bug Fixes

* **pendant:** preserve public policy and stable entity names ([#59](https://github.com/intigraph/chaski/issues/59)) ([200879e](https://github.com/intigraph/chaski/commit/200879e2bada02e5cb53516c2d4dae66f6f7ebe7))

## [0.4.0](https://github.com/intigraph/chaski/compare/v0.3.0...v0.4.0) (2026-11-05)


### Features

* ship repository-owned pointer pendants ([#34](https://github.com/intigraph/chaski/issues/34)) ([07e027f](https://github.com/intigraph/chaski/commit/07e027f4c76ca0b7defe7740a3eb9767d9906150))


### Bug Fixes

* **graph-sink:** expand compact Quipu prefixes before matching recovery reads ([#57](https://github.com/intigraph/chaski/issues/57)) ([14f65b4](https://github.com/intigraph/chaski/commit/14f65b45958ec66e70ee5c565b094a56498d0b28))
* **share:** pin a private embedding provider for repository builds ([#58](https://github.com/intigraph/chaski/issues/58)) ([f94a8fe](https://github.com/intigraph/chaski/commit/f94a8fe4e1c28d76e6f44c3e9673db07daeab6d6))

## [0.3.0](https://github.com/intigraph/chaski/compare/v0.2.4...v0.3.0) (2026-11-01)


### Features

* **cli:** chaski prime per the stack prime contract ([#48](https://github.com/intigraph/chaski/issues/48)) ([73b6482](https://github.com/intigraph/chaski/commit/73b648288857c17018fe5350dacdc5ecd3944da8))
* **github:** routing rules from GitHub activity facts to work requests ([#49](https://github.com/intigraph/chaski/issues/49)) ([83384e3](https://github.com/intigraph/chaski/commit/83384e33d74aedeb2d40ff45237a36326f273bc8))


### Bug Fixes

* **custody:** separate Darwin inode lease from SQLite file locks ([d75de60](https://github.com/intigraph/chaski/commit/d75de603e48b51350fbb1f1a87b535f1f2f6fc03))
* **stream:** resume durable change custody without startup polling ([#46](https://github.com/intigraph/chaski/issues/46)) ([dedfb16](https://github.com/intigraph/chaski/commit/dedfb16d54d3edec9127d3e56bf181b147f9a28e))


### Documentation

* **demo:** show the reactor preserving UNKNOWN state ([#53](https://github.com/intigraph/chaski/issues/53)) ([b03af67](https://github.com/intigraph/chaski/commit/b03af676e9dc2fdebe8affbf44e1f50389d04a9e))
* **readme:** embed the shared Thread Workshop hero ([#52](https://github.com/intigraph/chaski/issues/52)) ([aa2572b](https://github.com/intigraph/chaski/commit/aa2572bdb1aced5ad8c9dc8d742735ab80ef7575))
* **readme:** follow the shared stack sections ([#54](https://github.com/intigraph/chaski/issues/54)) ([22eecf0](https://github.com/intigraph/chaski/commit/22eecf0ad8536aa8b34c372723e9ffaa0cac1dc4))

## [0.2.4](https://github.com/intigraph/chaski/compare/v0.2.3...v0.2.4) (2026-10-25)


### Bug Fixes

* **inbox:** index entity routing to avoid disk sort spill ([#44](https://github.com/intigraph/chaski/issues/44)) ([6e112f4](https://github.com/intigraph/chaski/commit/6e112f48a9126b6e145ab454489709747326c9d4))

## [0.2.3](https://github.com/intigraph/chaski/compare/v0.2.2...v0.2.3) (2026-10-24)


### Bug Fixes

* consume multipart change transactions after verified commit ([#42](https://github.com/intigraph/chaski/issues/42)) ([7527131](https://github.com/intigraph/chaski/commit/75271313a0a93f9980d7af1f012695f4c6f57fdd))

## [0.2.2](https://github.com/intigraph/chaski/compare/v0.2.1...v0.2.2) (2026-10-23)


### Documentation

* synchronize the canonical stack overview ([#40](https://github.com/intigraph/chaski/issues/40)) ([1e88d7f](https://github.com/intigraph/chaski/commit/1e88d7f523fc9ec8f92a8974a11501025e74cba7))

## [0.2.1](https://github.com/intigraph/chaski/compare/v0.2.0...v0.2.1) (2026-10-22)


### Documentation

* synchronize the canonical stack overview ([#38](https://github.com/intigraph/chaski/issues/38)) ([4573e4f](https://github.com/intigraph/chaski/commit/4573e4fbc070c52039b67064c68612bf11c64a75))

## [0.2.0](https://github.com/scbrown/chaski/compare/v0.1.0...v0.2.0) (2026-10-21)


### Features

* block reactor on heap deadlines and matching change events ([#32](https://github.com/scbrown/chaski/issues/32)) ([6a7ecd7](https://github.com/scbrown/chaski/commit/6a7ecd72395e6ba68bbf2876dbe7594e86b91596))
* bridge admitted scheduled slots to signed-run ports ([#24](https://github.com/scbrown/chaski/issues/24)) ([ed83911](https://github.com/scbrown/chaski/commit/ed83911f3e24479e671768d27c74d32a0503fe18))
* **receiver:** bridge signed phases through the fixed Shuttle CLI ([#26](https://github.com/scbrown/chaski/issues/26)) ([87157ea](https://github.com/scbrown/chaski/commit/87157eaf14e593dcd971733a0a62d679085ece8c))
* **schedule:** compute weekly wall-clock UTC deadlines ([#36](https://github.com/scbrown/chaski/issues/36)) ([5dc2cc6](https://github.com/scbrown/chaski/commit/5dc2cc663bfc36fb042076bb797eed2e0dcadd20))
* **scheduled:** signed scheduled work chain — receiver, executor, sink factory, catalogue adapter (folds [#28](https://github.com/scbrown/chaski/issues/28)-[#31](https://github.com/scbrown/chaski/issues/31)) ([#31](https://github.com/scbrown/chaski/issues/31)) ([226aba0](https://github.com/scbrown/chaski/commit/226aba02df6272df3e78b4b5f8d3f85494f8f561))


### Bug Fixes

* require fresh bounded failure-family measurements ([#33](https://github.com/scbrown/chaski/issues/33)) ([fc3c017](https://github.com/scbrown/chaski/commit/fc3c017bdc91b3b08792577e5caf40b76f54904a))

## 0.1.0 (2026-10-15)


### Features

* a separable client label for sink writes; cap deliveries at 5 per tick (&lt;= 1 write/s) ([5e3758c](https://github.com/scbrown/chaski/commit/5e3758c552abebfe8923358b48c7b1e6b350008a))
* add scheduled failure-family alerts and retained leases ([#17](https://github.com/scbrown/chaski/issues/17)) ([60964ee](https://github.com/scbrown/chaski/commit/60964ee22a0c3bfacc4266b2979a4e1fb4000444))
* **consumer:** consume committed transaction streams with durable bounded delivery ([#15](https://github.com/scbrown/chaski/issues/15)) ([89e1f89](https://github.com/scbrown/chaski/commit/89e1f89b8c7019e5041d500929091b11159304a0))
* generalize emitters to any verdict pair; catch a same-verdict re-transition ([b10b4a0](https://github.com/scbrown/chaski/commit/b10b4a09fd305770d52276f9e08ee73c2375d922))
* graph sink — deliver emitter events as aegis:ReactionFiring in quipu ([a8e3f8b](https://github.com/scbrown/chaski/commit/a8e3f8b4b34bfd30d92164bb1d508dc7f14d5e75))
* graph sink — deliver emitter events as aegis:ReactionFiring in quipu ([ee12ba0](https://github.com/scbrown/chaski/commit/ee12ba08d445f0e94deab3f358b98751cf25b2a9))
* **graph_sink:** the read-back accepts eventId under the legacy or the quechua term (aegis-9dpcta.1) ([#4](https://github.com/scbrown/chaski/issues/4)) ([3f1d6a2](https://github.com/scbrown/chaski/commit/3f1d6a23960e15ab14fcfb2d9cf45534b1e89da6))
* keeper label on chaski_rule_firing (the rule owner) ([71df07d](https://github.com/scbrown/chaski/commit/71df07dd8f5823f7ee52de1f43ad8ad2bdd1de6a))
* stage 1 read-only reactor (event tail, schedule rules, metrics, canary) ([4b309df](https://github.com/scbrown/chaski/commit/4b309dfecd1592fcc5fec1f7124a2b970ffe2fd8))
* stage 2 emitters — verdict transitions to an outbox, delivered at least once ([68d2f85](https://github.com/scbrown/chaski/commit/68d2f850f397eec422bfd8758eab0304212a43d4))
* stage 2 emitters — verdict transitions to an outbox, delivered at least once ([543bc75](https://github.com/scbrown/chaski/commit/543bc75faf4640669a688dbefa0a72cb17e8a9bf))


### Bug Fixes

* bind emitter for the load_emitters annotation (ruff F821) ([3ee30d5](https://github.com/scbrown/chaski/commit/3ee30d59c97736c4c146cb7a8b59e37d3dd5daff))
* owner review — scope delivery per emitter, snapshot metrics, scrape ([88842e5](https://github.com/scbrown/chaski/commit/88842e53c40adc77a48d2343c86a16263d3a3884))
* refuse concurrent cursor and inbox consumers (aegis-byan1v) ([#18](https://github.com/scbrown/chaski/issues/18)) ([d68da48](https://github.com/scbrown/chaski/commit/d68da488b16d81647af0c12c02e58f9230cdc606))


### Documentation

* the absent-item known limit (sattler's review of 88842e53) ([43c7dc4](https://github.com/scbrown/chaski/commit/43c7dc41835786d65811365574df14cb00961d5d))
