use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.auto.conf', qq(
shared_preload_libraries = pg_streampack
pg_streampack.enabled = on
pg_streampack.min_size = 1
));
$node_primary->start;

# Take backup
my $backup_name = 'primary_backup';
$node_primary->backup($backup_name);

# just to reset compression stats
$node_primary->restart;

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name);
my $primary_conninfo = $node_primary->connstr . " options=''-c foo.bar=0''";
$node_standby->append_conf('postgresql.conf', qq(
primary_conninfo = '$primary_conninfo'
));
$node_standby->set_standby_mode;
$node_standby->start;

ok($node_primary->poll_query_until('postgres',
	"SELECT count(1) = 1 FROM pg_stat_replication WHERE application_name = 'standby'"),
	"standby didn't caught up");;

$node_primary->safe_psql('postgres',
	'CREATE TABLE test (id int not null primary key, name text)');

$node_primary->safe_psql('postgres',
	'INSERT INTO test SELECT a, repeat(a::text, a) FROM generate_series(1, 10000) AS a');

ok($node_standby->poll_query_until('postgres',
	"SELECT SUM(length(name)) = 199525505 FROM test"),
	"standby didn't caught up");;

$node_primary->safe_psql('postgres',
	'UPDATE test SET name = name || name');

ok($node_standby->poll_query_until('postgres',
	"SELECT SUM(length(name)) = 399051010 FROM test"),
	"standby didn't caught up");;

done_testing();
