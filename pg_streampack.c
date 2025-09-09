#include <lz4.h>

#include "postgres.h"

#include "common/connect.h"
#include "libpq/auth.h"
#include "libpq-fe.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "libpq/libpq.h"
#include "pqexpbuffer.h"
#include "libpq/pqformat.h"
#include "replication/walreceiver.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

PGDLLEXPORT void _PG_init(void);

#define LOGICAL_OPTIONS "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3"
#define COMPRESSION_OPTION "-c pg_streampack.requested=on"

static const uint32 header_size = 1 + 3 * sizeof(uint64);
static const uint32 new_header_size = header_size + sizeof(uint32);

/*
 * This server is configured to use compression.
 * It affects both, walsender and walreceiver.
 */
static bool pg_streampack_enabled = false;

/* replication client "requested" compression by passing GUC */
static bool pg_streampack_client_requested = false;

/* from which size we should try compressing messages. */
static int min_compress_size;

/*
 * Collect some statistic.
 * Will write it to log on shutdown.
 */
typedef struct shared_compression_stats
{
	pg_atomic_uint64 uncompressed;
	pg_atomic_uint64 compressed;
	pg_atomic_uint64 total;
	pg_atomic_uint64 increased;
} shared_compression_stats;

static shared_compression_stats *stats;

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Hackery to inject ourselves into walsender's stream.
 */
static ClientAuthentication_hook_type original_client_auth_hook = NULL;
static const PQcommMethods *OldPqCommMethods;

static void
socket_comm_reset(void)
{
	OldPqCommMethods->comm_reset();
}

static int
socket_flush(void)
{
	return OldPqCommMethods->flush();
}

static int
socket_flush_if_writable(void)
{
	return OldPqCommMethods->flush_if_writable();
}

static bool
socket_is_send_pending(void)
{
	return OldPqCommMethods->is_send_pending();
}

static int
socket_putmessage(char msgtype, const char *s, size_t len)
{
	return OldPqCommMethods->putmessage(msgtype, s, len);
}

static void
socket_putmessage_noblock(char msgtype, const char *s, size_t len)
{
	static PQExpBufferData output_buf = {0,};

	pg_atomic_fetch_add_u64(&stats->total, len);

	if (pg_streampack_client_requested &&
		pg_streampack_enabled && msgtype == 'd' &&
		len >= header_size + min_compress_size && s[0] == 'w')
	{
		int32 compressed_len;
		uint32 net_payload_len;
		uint32 payload_len = len - header_size;
		uint32 max_size = LZ4_compressBound(payload_len);

		if (unlikely(output_buf.data == NULL))
			initPQExpBuffer(&output_buf);
		else
			resetPQExpBuffer(&output_buf);
		enlargePQExpBuffer(&output_buf, max_size + new_header_size);
		if (PQExpBufferDataBroken(output_buf))
			goto send_uncompressed;

		/* We replace 'w' with 'z' to indicate compression. */
		appendPQExpBufferChar(&output_buf, 'z');
		/* copy other headers (3 * uint64) */
		appendBinaryPQExpBuffer(&output_buf, &s[1], header_size - 1);
		/*
		 * Save size of original payload. 4 bytes maybe too much for 128kB max,
		 * but in a future we can use two highest bits to store compression
		 * algorithm.
		 */
		net_payload_len = pg_hton32(payload_len);
		appendBinaryPQExpBuffer(&output_buf, (char *)&net_payload_len, sizeof(uint32));

		compressed_len =
			LZ4_compress_default(&s[header_size],
								 &output_buf.data[output_buf.len],
								 payload_len, max_size);

		if (compressed_len <= 0)
			goto send_uncompressed;

		if ((output_buf.len += compressed_len) < len)
		{
			/* Update timestamp, Since we "wasted" some time on compression. */
			uint64 net_ts = pg_hton64(GetCurrentTimestamp());
			memcpy(&output_buf.data[1 + 2 * sizeof(uint64)],
				   &net_ts, sizeof(uint64));

			/* call original function, which will actually send the message */
			OldPqCommMethods->putmessage_noblock(msgtype, output_buf.data, output_buf.len);
			pg_atomic_fetch_add_u64(&stats->compressed, output_buf.len);
			return;
		}
		else
			/* final message size is not getting smaller after compression */
			pg_atomic_fetch_add_u64(&stats->increased, 1);
	}

send_uncompressed:
	OldPqCommMethods->putmessage_noblock(msgtype, s, len);
	pg_atomic_fetch_add_u64(&stats->uncompressed, len);
}

#if PG_VERSION_NUM < 140000
static void
socket_startcopyout(void)
{
	OldPqCommMethods->startcopyout();
}

static void
socket_endcopyout(bool errorAbort)
{
	OldPqCommMethods->endcopyout(errorAbort);
}
#endif

static
#if PG_VERSION_NUM >= 120000
const
#endif
PQcommMethods PqCommSocketMethods = {
	socket_comm_reset,
	socket_flush,
	socket_flush_if_writable,
	socket_is_send_pending,
	socket_putmessage,
	socket_putmessage_noblock
#if PG_VERSION_NUM < 140000
	,
	socket_startcopyout,
	socket_endcopyout
#endif
};

static void
attach_to_walsender(Port *port, int status)
{
	/*
	 * Any other plugins which use ClientAuthentication_hook.
	 */
	if (original_client_auth_hook)
		original_client_auth_hook(port, status);

	if (am_walsender)
	{
		OldPqCommMethods = PqCommMethods;
		PqCommMethods = &PqCommSocketMethods;
	}
}

/*
 * Hackery to inject ourselves into walreceiver or logical replication worker
 * connections.
 */
static walrcv_startstreaming_fn old_walrcv_startstreaming;
static walrcv_endstreaming_fn old_walrcv_endstreaming;
static walrcv_connect_fn old_walrcv_connect;
static walrcv_receive_fn old_walrcv_receive;

/*
 * Private struct copied from libpqwalreceiver.c
 * Unfortunately we need to have it here...
 */
struct WalReceiverConn
{
	/* Current connection to the primary, if any */
	PGconn	   *streamConn;
	/* Used to remember if the connection is logical or physical */
	bool		logical;
	/* Buffer for currently read records */
	char	   *recvBuf;
	/* Whether this connection is currently in streaming mode */
	bool		streaming;
};

/*
 * Combination of libpqrcv_connect() and libpqrcv_check_conninfo() from
 * PostgreSQL 18, with small addition of compression "negotiation" which is
 * passed as GUC via options.
 *
 * Unfortunately it wasn't possible to use original function because it
 * overrides options provided inside conninfo string.
 */
static WalReceiverConn *
libpqrcv_connect(const char *conninfo,
#if PG_VERSION_NUM >= 170000
				 bool replication,
#endif
				 bool logical,
#if PG_VERSION_NUM >= 160000
				 bool must_use_password,
#endif
				 const char *appname, char **err)
{
	WalReceiverConn *conn;
	const char *keys[6];
	const char *vals[6];
	int			i = 0;
	PQconninfoOption *opts = NULL;
	char	   *options = NULL;
	char	   *conn_options = NULL;
#if PG_VERSION_NUM < 170000
	bool		replication = true;
#endif
#if PG_VERSION_NUM < 160000
	bool		must_use_password = false;
#endif
	bool		uses_password = false;

	/*
	 * We use the expand_dbname parameter to process the connection string (or
	 * URI), and pass some extra options.
	 */
	keys[i] = "dbname";
	vals[i] = conninfo;

	/* We can not have logical without replication */
	Assert(replication || !logical);

	opts = PQconninfoParse(conninfo, err);
	if (opts == NULL)
	{
		/* The error string is malloc'd, so we must free it explicitly */
		char	   *errcopy = *err ? pstrdup(*err) : "out of memory";

		PQfreemem(*err);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid connection string syntax: %s", errcopy)));
	}

	for (PQconninfoOption *opt = opts; opt->keyword != NULL; ++opt)
	{
		/* Ignore connection options that are not present. */
		if (opt->val == NULL)
			continue;

		if (strcmp(opt->keyword, "options") == 0 && opt->val[0] != '\0')
			conn_options = opt->val;
		else if (strcmp(opt->keyword, "password") == 0 && opt->val[0] != '\0')
			uses_password = true;
	}

	if (must_use_password && !uses_password)
	{
		/* malloc'd, so we must free it explicitly */
		PQconninfoFree(opts);

		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("password is required"),
				 errdetail("Non-superusers must provide a password in the connection string.")));
	}

	if (replication)
	{
		keys[++i] = "replication";
		vals[i] = logical ? "database" : "true";

		if (logical)
		{
			/* Tell the publisher to translate to our encoding */
			keys[++i] = "client_encoding";
			vals[i] = GetDatabaseEncodingName();

			/*
			 * Force assorted GUC parameters to settings that ensure that the
			 * publisher will output data values in a form that is unambiguous
			 * to the subscriber.  (We don't want to modify the subscriber's
			 * GUC settings, since that might surprise user-defined code
			 * running in the subscriber, such as triggers.)  This should
			 * match what pg_dump does.
			 */
			keys[++i] = "options";
			vals[i] = pg_streampack_enabled ? LOGICAL_OPTIONS " " COMPRESSION_OPTION : LOGICAL_OPTIONS;
		}
		else
		{
			/*
			 * The database name is ignored by the server in replication mode,
			 * but specify "replication" for .pgpass lookup.
			 */
			keys[++i] = "dbname";
			vals[i] = "replication";
			if (pg_streampack_enabled)
			{
				options = palloc0(strlen(COMPRESSION_OPTION) + (conn_options ? strlen(conn_options) + 1 : 0) + 1);
				if (conn_options)
				{
					strcpy(options, conn_options);
					strcat(options, " ");
				}
				strcat(options, COMPRESSION_OPTION);

				keys[++i] = "options";
				vals[i] = options;
			}
		}
	}

	PQconninfoFree(opts);

	keys[++i] = "fallback_application_name";
	vals[i] = appname;

	keys[++i] = NULL;
	vals[i] = NULL;

	Assert(i < lengthof(keys));

	conn = palloc0(sizeof(WalReceiverConn));
	conn->streamConn =
		libpqsrv_connect_params(keys, vals,
								 /* expand_dbname = */ true,
								WAIT_EVENT_LIBPQWALRECEIVER_CONNECT);

	if (options != NULL)
		pfree(options);

	if (PQstatus(conn->streamConn) != CONNECTION_OK)
		goto bad_connection_errmsg;

	if (must_use_password && !PQconnectionUsedPassword(conn->streamConn))
	{
		libpqsrv_disconnect(conn->streamConn);
		pfree(conn);

		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("password is required"),
				 errdetail("Non-superuser cannot connect if the server does not request a password."),
				 errhint("Target server's authentication method must be changed, or set password_required=false in the subscription parameters.")));
	}

	/*
	 * Set always-secure search path for the cases where the connection is
	 * used to run SQL queries, so malicious users can't get control.
	 */
	if (!replication || logical)
	{
		PGresult   *res;

		res = libpqsrv_exec(conn->streamConn,
							ALWAYS_SECURE_SEARCH_PATH_SQL,
							WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			*err = psprintf(_("could not clear search path: %s"),
							pchomp(PQerrorMessage(conn->streamConn)));
			goto bad_connection;
		}
		PQclear(res);
	}

	conn->logical = logical;

	return conn;

	/* error path, using libpq's error message */
bad_connection_errmsg:
	*err = pchomp(PQerrorMessage(conn->streamConn));

	/* error path, error already set */
bad_connection:
	libpqsrv_disconnect(conn->streamConn);
	pfree(conn);
	return NULL;
}

static bool
libpqrcv_startstreaming(WalReceiverConn *conn,
						const WalRcvStreamOptions *options)
{
	return conn->streaming = old_walrcv_startstreaming(conn, options);
}

static void
libpqrcv_endstreaming(WalReceiverConn *conn, TimeLineID *next_tli)
{
	conn->streaming = false;
	old_walrcv_endstreaming(conn, next_tli);
}

static int
libpqrcv_receive(WalReceiverConn *conn, char **buffer,
				 pgsocket *wait_fd)
{
	static StringInfoData input_buf = {0,};
	int len = old_walrcv_receive(conn, buffer, wait_fd);

	if (!conn->streaming)
		return len;

	/* 'z' indicates that we received compressed message */
	if (*buffer && len >= new_header_size && *buffer[0] == 'z')
	{
		uint32 uncompressed_len;
		uint32 payload_len = len - new_header_size;
		char *buf = *buffer;
		MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);

		/* get uncompressed size of payload from headers */
		memcpy(&uncompressed_len, &buf[header_size], sizeof(uint32));
		/* reserve two high bits for compression algorithm */
		uncompressed_len = pg_ntoh32(uncompressed_len) & 0x3FFFFFFF;

		/* Calculate length of uncompressed message. */
		len = uncompressed_len + header_size;

		if (unlikely(input_buf.data == NULL))
			initStringInfo(&input_buf);
		else
			resetStringInfo(&input_buf);
		enlargeStringInfo(&input_buf, len);

		MemoryContextSwitchTo(old_ctx);

		/* copy headers */
		pq_sendbyte(&input_buf, 'w');
		memcpy(&input_buf.data[1], &buf[1], header_size - 1);

		/* uncompress payload */
		if (LZ4_decompress_safe(&buf[new_header_size],
								&input_buf.data[header_size],
								payload_len, uncompressed_len) != uncompressed_len)
			ereport(ERROR, (errmsg("failed to decompress replication message")));

		*buffer = input_buf.data;
	}

	return len;
}

/* allow setting pg_streampack.requested only from client connection */
static bool
compression_requested_check_hook(bool *newval, void **extra, GucSource source)
{
	return source == PGC_S_DEFAULT || source == PGC_S_CLIENT;
}

static Size
memsize()
{
	Size size = MAXALIGN(sizeof(shared_compression_stats));
	return size;
}

static void
shmem_shutdown(int code, Datum arg)
{
	elog(LOG,
		 "pg_streampack state: total: %lu bytes, uncompressed: %lu bytes, compressed: %lu bytes, compression increased size: %lu times",
		 pg_atomic_read_u64(&stats->total),
		 pg_atomic_read_u64(&stats->uncompressed),
		 pg_atomic_read_u64(&stats->compressed),
		 pg_atomic_read_u64(&stats->increased));
}

static void
shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	stats = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	stats = ShmemInitStruct("pg_streampack", sizeof(*stats), &found);
	if (!found)
	{
		pg_atomic_init_u64(&stats->uncompressed, 0);
		pg_atomic_init_u64(&stats->compressed, 0);
		pg_atomic_init_u64(&stats->total, 0);
		pg_atomic_init_u64(&stats->increased, 0);
	}

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
		on_shmem_exit(shmem_shutdown, (Datum)0);
}

static void
shmem_request(void)
{
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(memsize());
}

void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;

	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "pg_streampack is not in shared_preload_libraries");

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = shmem_request;
#else
	shmem_request();
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = shmem_startup;

	original_client_auth_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = attach_to_walsender;

	/*
	 * Load libpqwalreceiver, it will set WalReceiverFunctions which we want to
	 * intercept. Normally it is loaded by walreceiver or logical replication
	 * worker processes, but there are no hooks available.
	 * */
	load_file("libpqwalreceiver", false);

	old_walrcv_connect = WalReceiverFunctions->walrcv_connect;
	WalReceiverFunctions->walrcv_connect = libpqrcv_connect;

	old_walrcv_startstreaming = WalReceiverFunctions->walrcv_startstreaming;
	WalReceiverFunctions->walrcv_startstreaming = libpqrcv_startstreaming;

	old_walrcv_endstreaming = WalReceiverFunctions->walrcv_endstreaming;
	WalReceiverFunctions->walrcv_endstreaming = libpqrcv_endstreaming;

	old_walrcv_receive = WalReceiverFunctions->walrcv_receive;
	WalReceiverFunctions->walrcv_receive = libpqrcv_receive;

	DefineCustomBoolVariable("pg_streampack.enabled",
							 "Enable compression of replication messages.",
							 NULL,
							 &pg_streampack_enabled,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_streampack.requested",
							 "Compression of replication messages was requested by client.",
							 NULL,
							 &pg_streampack_client_requested,
							 false,
							 PGC_BACKEND,
							 GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL,
							 compression_requested_check_hook,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_streampack.min_size",
							"Minimal size of payload to compress.",
							"",
							&min_compress_size,
							128,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);
	EmitWarningsOnPlaceholders("pg_streampack");
}
