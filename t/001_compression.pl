use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

sub check_caught_up
{
	my ($node, $value) = @_;

	ok($node->poll_query_until('postgres',
		"SELECT SUM(length(name)) = $value FROM test"),
		$node->name . " caught up");
}

sub check_log
{
	my ($node, $position, $pattern, $msg) = @_;

	my $found = 0;
	foreach my $i (0 .. 100)
	{
		if ($node->log_contains($pattern, $position))
		{
			$found = 1;
			last;
		}
		usleep(100_000);
	}
	is($found, 1, $msg);
}

my $config = qq(
wal_level = logical
shared_preload_libraries = 'pg_stat_statements,pg_streampack'
pg_streampack.enabled = on
pg_streampack.min_size = 1
);

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.auto.conf', $config);
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

my $node_logical = PostgreSQL::Test::Cluster->new('logical');
$node_logical->init;
$node_logical->append_conf('postgresql.auto.conf', $config);
$node_logical->start;

$node_primary->safe_psql('postgres',
	"CREATE USER repl WITH REPLICATION PASSWORD 'repl'");

ok($node_primary->poll_query_until('postgres',
	"SELECT count(1) = 1 FROM pg_stat_replication WHERE application_name = 'standby'"),
	"standby is streaming");

$node_primary->safe_psql('postgres',
	'CREATE TABLE test (id int not null primary key, name text)');

$node_primary->safe_psql('postgres',
	'CREATE PUBLICATION test FOR ALL TABLES');

$node_primary->safe_psql('postgres',
	'INSERT INTO test SELECT a, repeat(a::text, a) FROM generate_series(1, 10000) AS a');

check_caught_up($node_standby, '199525505');

$node_logical->safe_psql('postgres',
	'CREATE TABLE test (id int not null primary key, name text)');

$node_logical->safe_psql('postgres',
	"CREATE SUBSCRIPTION test CONNECTION '$primary_conninfo' PUBLICATION test");

check_caught_up($node_logical, '199525505');

$node_primary->safe_psql('postgres',
	'UPDATE test SET name = name || name');

check_caught_up($node_standby, '399051010');
check_caught_up($node_logical, '399051010');

if (int($node_logical->safe_psql('postgres',
		"SELECT current_setting('server_version_num')::int/10000")) >= 16)
{
	$node_logical->safe_psql('postgres', "CREATE USER repl");
	$node_logical->safe_psql('postgres',
		"ALTER SUBSCRIPTION test SET (PASSWORD_REQUIRED)");
	my $log_size = -s $node_logical->logfile;
	$node_logical->safe_psql('postgres', "ALTER SUBSCRIPTION test OWNER TO repl");

	check_log($node_logical, $log_size,
		"Non-superusers must provide a password in the connection string",
		'must provide a password');

	$log_size = -s $node_logical->logfile;
	$node_logical->safe_psql('postgres',
		"ALTER SUBSCRIPTION test CONNECTION '$primary_conninfo user=repl password=repl'");

	check_log($node_logical, $log_size,
		"Non-superuser cannot connect if the server does not request a password",
		'must provide a password');
}

my $log_size = -s $node_logical->logfile;
$node_logical->safe_psql('postgres',
	"UPDATE pg_subscription SET subconninfo = 'broken'");

check_log($node_logical, $log_size,
	"invalid connection string syntax:",
	'invalid connection string');

$node_logical->stop;
$log_size = -s $node_primary->logfile;
$node_primary->stop;
check_log($node_primary, $log_size,
	'pg_streampack state: total: [1-9]\d* bytes, uncompressed: \d+ bytes, compressed: [1-9]\d+ bytes, compression increased size: \d+ times',
	'compression works');

done_testing();
