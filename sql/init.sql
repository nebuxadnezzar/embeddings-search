 -- sqlite3 -init init.sql
--
CREATE TABLE IF NOT EXISTS pfx(id INTEGER PRIMARY KEY AUTOINCREMENT , prefix TEXT);
create table if not exists tmp(prefix);
-- .mode line
.import './data/sdn-prefixes.txt' tmp
insert into pfx(prefix) select * from tmp;


