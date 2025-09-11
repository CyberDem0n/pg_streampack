#include <lz4.h>

#include "postgres.h"

#include "common/connect.h"
#include "libpq/auth.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "libpq/libpq.h"
#include "pqexpbuffer.h"
#include "replication/walreceiver.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

PGDLLEXPORT void _PG_init(void);

#define LZ4_DICT_SIZE 65536
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

typedef struct
{
	PQExpBufferData *buf;
	char *lz4_dict;
	LZ4_stream_t *stream;
} compression_ctx;

static compression_ctx *comp_ctx = NULL;

typedef struct
{
	PQExpBufferData *buf;
	char *lz4_dict;
	LZ4_streamDecode_t *stream;
} decompression_ctx;

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
free_comp_ctx(compression_ctx *ctx)
{
	if (ctx == NULL)
		return;
	if (ctx->stream)
		LZ4_freeStream(ctx->stream);
	if (ctx->lz4_dict)
		free(ctx->lz4_dict);
	destroyPQExpBuffer(ctx->buf);
	free(ctx);
}

static compression_ctx *
init_comp_ctx(void)
{
	compression_ctx *ctx = malloc(sizeof(compression_ctx));
	if (ctx == NULL)
		goto cleanup;

	memset(ctx, 0, sizeof(compression_ctx));

	ctx->buf = createPQExpBuffer();
	if (ctx->buf == NULL)
		goto cleanup;

	ctx->lz4_dict = malloc(LZ4_DICT_SIZE);
	if (ctx->lz4_dict == NULL)
		goto cleanup;

	ctx->stream = LZ4_createStream();
	if (ctx->stream == NULL)
		goto cleanup;

	return ctx;

cleanup:
	free_comp_ctx(ctx);

	return NULL;;
}

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
	pg_atomic_fetch_add_u64(&stats->total, len);

	if (comp_ctx != NULL && msgtype == 'd' &&
		pg_streampack_client_requested &&
		len >= header_size + min_compress_size && s[0] == 'w')
	{
		uint64 net_ts;
		int32 compressed_len;
		uint32 dict_size, net_payload_len;
		uint32 payload_len = len - header_size;
		uint32 max_size = LZ4_compressBound(payload_len);
		compression_ctx *ctx = comp_ctx;
		PQExpBufferData *buf = ctx->buf;

		resetPQExpBuffer(buf);
		enlargePQExpBuffer(buf, new_header_size + max_size);
		if (PQExpBufferDataBroken(*buf))
			goto send_uncompressed;

		/* We replace 'w' with 'z' to indicate compression. */
		appendPQExpBufferChar(buf, 'z');
		/* copy other headers (3 * uint64) */
		appendBinaryPQExpBuffer(buf, &s[1], header_size - 1);

		/*
		 * Save size of original payload. 4 bytes maybe too much for 128kB max,
		 * but in a future we can use two highest bits to store compression
		 * algorithm.
		 */
		net_payload_len = pg_hton32(payload_len);
		appendBinaryPQExpBuffer(buf, (void *)&net_payload_len, sizeof(uint32));

		compressed_len =
			LZ4_compress_fast_continue(ctx->stream, &s[header_size],
									   &buf->data[buf->len],
									   payload_len, max_size, 1); /* default acceleration */
		if (compressed_len <= 0)
		{
			/* If streaming compression failed switch it off. */
			free_comp_ctx(comp_ctx);
			comp_ctx = NULL;
			elog(LOG_SERVER_ONLY, "LZ4 compression failed: %d", compressed_len);
			goto send_uncompressed;
		}

		dict_size = Min(payload_len, LZ4_DICT_SIZE);
		memcpy(ctx->lz4_dict, &s[len - dict_size],dict_size);
		LZ4_loadDict(ctx->stream, ctx->lz4_dict, dict_size);

		if ((buf->len += compressed_len) > len)
			/* final message size is not getting smaller after compression */
			pg_atomic_fetch_add_u64(&stats->increased, 1);

		/* Update timestamp, Since we "wasted" some time on compression. */
		net_ts = pg_hton64(GetCurrentTimestamp());
		memcpy(&buf->data[1 + 2 * sizeof(uint64)], &net_ts, sizeof(uint64));

		/* call original function, which will actually send the message */
		OldPqCommMethods->putmessage_noblock(msgtype, buf->data, buf->len);
		pg_atomic_fetch_add_u64(&stats->compressed, buf->len);
		return;
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
on_exit_callback(int code, Datum arg)
{
	free_comp_ctx(comp_ctx);
}

static void
attach_to_walsender(Port *port, int status)
{
	/*
	 * Any other plugins which use ClientAuthentication_hook.
	 */
	if (original_client_auth_hook)
		original_client_auth_hook(port, status);

	if (am_walsender && pg_streampack_enabled &&
		(comp_ctx = init_comp_ctx()) != NULL)
	{
		on_proc_exit(on_exit_callback, 0);
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
static walrcv_disconnect_fn old_walrcv_disconnect;

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
	/* Decompression context */
	decompression_ctx *decomp_ctx;
	/* Whether this connection is currently in streaming mode */
	bool		streaming;
};

static void
free_decomp_ctx(decompression_ctx *ctx)
{
	if (ctx == NULL)
		return;
	if (ctx->stream)
		LZ4_freeStreamDecode(ctx->stream);
	if (ctx->lz4_dict)
		free(ctx->lz4_dict);
	destroyPQExpBuffer(ctx->buf);
	free(ctx);
}

static decompression_ctx *
init_decomp_ctx(void)
{
	decompression_ctx *ctx = malloc(sizeof(decompression_ctx));
	if (ctx == NULL)
		goto cleanup;

	memset(ctx, 0, sizeof(compression_ctx));

	ctx->buf = createPQExpBuffer();
	if (ctx->buf == NULL)
		goto cleanup;

	ctx->lz4_dict = malloc(LZ4_DICT_SIZE);
	if (ctx->lz4_dict == NULL)
		goto cleanup;

	ctx->stream = LZ4_createStreamDecode();
	if (ctx->stream == NULL)
		goto cleanup;

	return ctx;

cleanup:
	free_decomp_ctx(ctx);

	return NULL;
}

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
	decompression_ctx *ctx = pg_streampack_enabled ? init_decomp_ctx() : NULL;

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
			vals[i] = ctx == NULL ? LOGICAL_OPTIONS : LOGICAL_OPTIONS " " COMPRESSION_OPTION;
		}
		else
		{
			/*
			 * The database name is ignored by the server in replication mode,
			 * but specify "replication" for .pgpass lookup.
			 */
			keys[++i] = "dbname";
			vals[i] = "replication";

			if (ctx != NULL)
			{
				options = palloc0(1 + strlen(COMPRESSION_OPTION) +
								  (conn_options ? strlen(conn_options) + 1 : 0));
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
	conn->decomp_ctx = ctx;

	return conn;

	/* error path, using libpq's error message */
bad_connection_errmsg:
	*err = pchomp(PQerrorMessage(conn->streamConn));

	/* error path, error already set */
bad_connection:
	libpqsrv_disconnect(conn->streamConn);
	pfree(conn);
	free_decomp_ctx(ctx);
	return NULL;
}

static bool
libpqrcv_startstreaming(WalReceiverConn *conn,
						const WalRcvStreamOptions *options)
{
	if (!old_walrcv_startstreaming(conn, options))
		return false;

	conn->streaming = conn->decomp_ctx != NULL;

	return true;
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
	int len = old_walrcv_receive(conn, buffer, wait_fd);

	if (!conn->streaming)
		return len;

	/* 'z' indicates that we received compressed message */
	if (*buffer && len >= new_header_size && *buffer[0] == 'z')
	{
		int decompressed_len;
		uint32 dict_size, expected_len;
		uint32 payload_len = len - new_header_size;
		decompression_ctx *ctx = conn->decomp_ctx;
		PQExpBufferData *buf = ctx->buf;

		/* get uncompressed size of payload from headers */
		memcpy(&expected_len, &buffer[0][header_size], sizeof(uint32));
		/* reserve two high bits for compression algorithm */
		expected_len = pg_ntoh32(expected_len) & 0x3FFFFFFF;

		/* Calculate length of uncompressed message. */
		len = header_size + expected_len;

		resetPQExpBuffer(buf);
		enlargePQExpBuffer(buf, len);
		if (PQExpBufferDataBroken(*buf))
			ereport(ERROR, (errmsg("Failed to allocate %u bytes", len)));

		/* Restore 'w', it was replaced with 'z'. */
		appendPQExpBufferChar(buf, 'w');
		/* copy other headers (3 * uint64) */
		appendBinaryPQExpBuffer(buf, &buffer[0][1], header_size - 1);

		/* uncompress payload */
		decompressed_len =
			LZ4_decompress_safe_continue(ctx->stream,
										 &buffer[0][new_header_size],
										 &buf->data[header_size],
										 payload_len, expected_len);
		if (decompressed_len != expected_len)
			ereport(ERROR,
					(errmsg("Failed to decompress replication message: payload_len = %u, expected_len: %u, got: %d\n",
							payload_len, expected_len, decompressed_len)));

		dict_size = Min(decompressed_len, LZ4_DICT_SIZE);
		memcpy(ctx->lz4_dict, &buf->data[len - dict_size], dict_size);
		LZ4_setStreamDecode(ctx->stream, ctx->lz4_dict, dict_size);

		*buffer = buf->data;
	}

	return len;
}

static void
libpqrcv_disconnect(WalReceiverConn *conn)
{
	free_decomp_ctx(conn->decomp_ctx);

	old_walrcv_disconnect(conn);
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

	old_walrcv_disconnect = WalReceiverFunctions->walrcv_disconnect;
	WalReceiverFunctions->walrcv_disconnect = libpqrcv_disconnect;

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
