#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif
#ifdef USE_ZSTD
#include <zstd.h>
#endif

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
#include "utils/varlena.h"

#if PG_VERSION_NUM >= 180000
PG_MODULE_MAGIC_EXT(.name = "pg_streampack", .version = "0.1");
#else
PG_MODULE_MAGIC;
#endif

PGDLLEXPORT void _PG_init(void);

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

#define LZ4_DICT_SIZE 65536
#define LOGICAL_OPTIONS "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3"
#define COMPRESSION_OPTION "-c pg_streampack.requested="

/*
 * Offsets/sizes for replication XLogData message:
 *   1 byte message type ('w')
 *   8 bytes walStart (network byte order)
 *   8 bytes walEnd (network byte order)
 *   8 bytes sendTime (network byte order)
 *
 * We steal the most significant bit of the first byte of the network-order
 * sendTime field to indicate that the message payload is LZ4-compressed and
 * that we have appended a 4-byte uncompressed length immediately after the
 * normal 25-byte header. The receiver clears that bit before exposing the
 * header upward.
 */
static const uint32 ts_offset = 1 + 2 * sizeof(uint64);				/* byte offset of sendTime field */
static const uint32 header_size = ts_offset + sizeof(uint64);		/* 25 bytes total */
static const uint32 new_header_size = header_size + sizeof(uint32);	/* +4 bytes (uncompressed length) for compressed msg */

#define PG_STREAMPACK_FLAG_MASK 0x80

typedef enum
{
	COMPRESS_ZSTD = 1,
	COMPRESS_LZ4 = 2,
	COMPRESS_MAX_ID = 2
} pg_streampack_algo;

static const struct config_enum_entry algo_options[] = {
#ifdef USE_ZSTD
	{"zstd", COMPRESS_ZSTD},
#endif
#ifdef USE_LZ4
	{"lz4", COMPRESS_LZ4},
#endif
};

/*
 * We reserve two high bits in uncompressed length header for compression
 * algorithm. Based on COMPRESSION_ALGO_SHIFT we also calculate mask.
 */
#define COMPRESSION_ALGO_SHIFT 30

/* pg_streampack.compression GUC */
static char *pg_streampack_compression_string = NULL;
/*
 * We use pg_streampack_compression_configured as a hash to quickly check which
 * algorithms are configured in pg_streampack.compression.
 */
static bool pg_streampack_compression_configured[COMPRESS_MAX_ID + 1] = {0,};
/* List of algorithms from pg_streampack.compression GUC after parsing */
static uint32 pg_streampack_compression_order[COMPRESS_MAX_ID] = {0,};

/* replication client "requested" compression by passing GUC */
static char *pg_streampack_client_requested_string = 0;

/* from which size we should try compressing messages. */
static int min_compress_size;

static bool
pg_streampack_enabled(void)
{
	return pg_streampack_compression_order[0] > 0;
}

static char *
pg_streampack_compression_request(void)
{
	static char ret[16];

	memset(ret, 0, sizeof(ret));
	for (int i = 0; i < COMPRESS_MAX_ID && pg_streampack_compression_order[i]; i++)
	{
		if (i > 0)
			strcat(ret, ",");
		for (int j = 0; j < lengthof(algo_options); j++)
		{
			struct config_enum_entry option = algo_options[j];

			if (option.val == pg_streampack_compression_order[i])
			{
				strcat(ret, option.name);
				break;
			}
		}
	}

	return ret;
}

typedef struct
{
	PQExpBufferData *buf;
#ifdef USE_LZ4
	char *lz4_dict;
	LZ4_stream_t *lz4_stream;
#endif
#ifdef USE_ZSTD
	ZSTD_CStream *zstd_stream;
#endif
	bool (*compress) (const char *, uint32);
} compression_ctx;

static compression_ctx *comp_ctx = NULL;

/*
 * Hackery to inject ourselves into walsender's stream.
 */
static ClientAuthentication_hook_type original_client_auth_hook = NULL;
static const PQcommMethods *OldPqCommMethods;

static void
free_comp_ctx_lz4(compression_ctx *ctx)
{
#ifdef USE_LZ4
	if (ctx->lz4_stream != NULL)
	{
		LZ4_freeStream(ctx->lz4_stream);
		ctx->lz4_stream = NULL;
	}

	if (ctx->lz4_dict != NULL)
	{
		free(ctx->lz4_dict);
		ctx->lz4_dict = NULL;
	}
#endif
}

static void
free_comp_ctx_ztsd(compression_ctx *ctx)
{
#ifdef USE_ZSTD
	if (ctx->zstd_stream != NULL)
	{
		ZSTD_freeCStream(ctx->zstd_stream);
		ctx->zstd_stream = NULL;
	}
#endif
}

static void
free_comp_ctx(compression_ctx *ctx)
{
	if (ctx == NULL)
		return;
	free_comp_ctx_ztsd(ctx);
	free_comp_ctx_lz4(ctx);
	destroyPQExpBuffer(ctx->buf);
	free(ctx);
}

static compression_ctx *
init_comp_ctx(void)
{
	bool success = false;
	compression_ctx *ctx = malloc(sizeof(compression_ctx));
	if (ctx == NULL)
		goto cleanup;

	memset(ctx, 0, sizeof(*ctx));

#ifdef USE_LZ4
	if (pg_streampack_compression_configured[COMPRESS_LZ4] > 0)
	{
		if ((ctx->lz4_dict = malloc(LZ4_DICT_SIZE)) == NULL ||
			(ctx->lz4_stream = LZ4_createStream()) == NULL)
			free_comp_ctx_lz4(ctx);
		else success = true;
	}
#endif

#ifdef USE_ZSTD
	if (pg_streampack_compression_configured[COMPRESS_ZSTD] > 0)
	{
		if ((ctx->zstd_stream = ZSTD_createCStream()) == NULL ||
			ZSTD_isError(ZSTD_initCStream(ctx->zstd_stream, 1)))
			free_comp_ctx_ztsd(ctx);
		else success = true;
	}
#endif

	if (!success)
		goto cleanup;

	if ((ctx->buf = createPQExpBuffer()) == NULL)
		goto cleanup;

	return ctx;

cleanup:
	free_comp_ctx(ctx);

	return NULL;
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

#if USE_LZ4 || USE_ZSTD
static void
append_new_header(uint32 algo, uint32 payload_len)
{
	uint32 net_payload_len = pg_hton32(((algo -1) << COMPRESSION_ALGO_SHIFT) | payload_len);
	appendBinaryPQExpBuffer(comp_ctx->buf, (void *)&net_payload_len, sizeof(uint32));
}
#endif

#ifdef USE_LZ4
static bool
pg_streampack_compress_lz4(const char *payload, uint32 payload_len)
{
	int32 compressed_len;
	uint32 dict_size;
	compression_ctx *ctx = comp_ctx;
	PQExpBufferData *buf = ctx->buf;
	uint32 max_size = LZ4_compressBound(payload_len);

	enlargePQExpBuffer(buf, new_header_size + max_size);
	if (PQExpBufferDataBroken(*buf))
		return false;

	append_new_header(COMPRESS_LZ4, payload_len);

	compressed_len =
		LZ4_compress_fast_continue(ctx->lz4_stream, payload,
								   &buf->data[buf->len], payload_len,
								   max_size, 1); /* default acceleration */
	if (compressed_len <= 0)
	{
		/* If streaming compression failed switch it off. */
		free_comp_ctx(comp_ctx);
		comp_ctx = NULL;
		elog(LOG_SERVER_ONLY, "LZ4 compression failed: %d", compressed_len);
		return false;
	}

	dict_size = Min(payload_len, LZ4_DICT_SIZE);
	memcpy(ctx->lz4_dict, &payload[payload_len - dict_size], dict_size);
	LZ4_loadDict(ctx->lz4_stream, ctx->lz4_dict, dict_size);

	buf->len += compressed_len;
	return true;
}
#endif

#ifdef USE_ZSTD
static bool
pg_streampack_compress_zstd(const char *payload, uint32 payload_len)
{
	compression_ctx *ctx = comp_ctx;
	PQExpBufferData *buf = ctx->buf;
	uint32 max_size = ZSTD_compressBound(payload_len);

	enlargePQExpBuffer(buf, new_header_size + max_size);
	if (PQExpBufferDataBroken(*buf))
		return false;

	append_new_header(COMPRESS_ZSTD, payload_len);

	{
		ZSTD_inBuffer input = {payload, payload_len, 0};
		ZSTD_outBuffer output = {&buf->data[buf->len], max_size, 0};
		size_t result = ZSTD_compressStream2(ctx->zstd_stream, &output,
											 &input, ZSTD_e_flush);

		if (ZSTD_isError(result))
		{
			/* If streaming compression failed switch it off. */
			free_comp_ctx(comp_ctx);
			comp_ctx = NULL;
			elog(LOG_SERVER_ONLY, "ZSTD compression failed: %s",
				 ZSTD_getErrorName(result));
			return false;
		}
		buf->len += output.pos;
	}

	return true;
}
#endif

static void
socket_putmessage_noblock(char msgtype, const char *s, size_t len)
{
	pg_atomic_fetch_add_u64(&stats->total, len);

	if (comp_ctx != NULL && comp_ctx->compress != NULL && msgtype == 'd' &&
		len >= header_size + min_compress_size && s[0] == 'w')
	{
		uint64 net_ts;
		uint32 payload_len = len - header_size;
		compression_ctx *ctx = comp_ctx;
		PQExpBufferData *buf = ctx->buf;

		resetPQExpBuffer(buf);

		/* Copy original replication header (msgtype + 3 * uint64)  */
		appendBinaryPQExpBuffer(buf, s, header_size);

		if (!ctx->compress(&s[header_size], payload_len))
			goto send_uncompressed;

		if (buf->len > len)
			/* final message size is not getting smaller after compression */
			pg_atomic_fetch_add_u64(&stats->increased, 1);

		/*
		 * Update timestamp, since we "wasted" some time on compression.
		 * Set the most significant bit as an indicator that message is compressed.
		 */
		net_ts = pg_hton64(GetCurrentTimestamp() | (((uint64) PG_STREAMPACK_FLAG_MASK) << 56));
		memcpy(&buf->data[ts_offset], &net_ts, sizeof(uint64));

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

	if (am_walsender && pg_streampack_enabled() &&
		(comp_ctx = init_comp_ctx()) != NULL)
	{
		on_proc_exit(on_exit_callback, 0);
		OldPqCommMethods = PqCommMethods;
		PqCommMethods = &PqCommSocketMethods;
	}
}

typedef struct decompression_ctx
{
	PQExpBufferData *buf;
#ifdef USE_LZ4
	char *lz4_dict;
	LZ4_streamDecode_t *lz4_stream;
#endif
#ifdef USE_ZSTD
	ZSTD_DStream *zstd_stream;
#endif
	void (*decompress) (struct decompression_ctx *, uint32, const char *, uint32);
} decompression_ctx;

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
free_decomp_ctx_lz4(decompression_ctx *ctx)
{
#ifdef USE_LZ4
	if (ctx->lz4_stream != NULL)
	{
		LZ4_freeStreamDecode(ctx->lz4_stream);
		ctx->lz4_stream = NULL;
	}

	if (ctx->lz4_dict != NULL)
	{
		free(ctx->lz4_dict);
		ctx->lz4_dict = NULL;
	}
#endif
}

static void
free_decomp_ctx_zstd(decompression_ctx *ctx)
{
#ifdef USE_ZSTD
	if (ctx->zstd_stream != NULL)
	{
		ZSTD_freeDStream(ctx->zstd_stream);
		ctx->zstd_stream = NULL;
	}
#endif
}

static void
free_decomp_ctx(decompression_ctx *ctx)
{
	if (ctx == NULL)
		return;
	free_decomp_ctx_zstd(ctx);
	free_decomp_ctx_lz4(ctx);
	destroyPQExpBuffer(ctx->buf);
	free(ctx);
}

static decompression_ctx *
init_decomp_ctx(void)
{
	bool success = false;
	decompression_ctx *ctx = malloc(sizeof(decompression_ctx));
	if (ctx == NULL)
		goto cleanup;

	memset(ctx, 0, sizeof(*ctx));

#ifdef USE_LZ4
	if (pg_streampack_compression_configured[COMPRESS_LZ4] > 0)
	{
		if ((ctx->lz4_dict = malloc(LZ4_DICT_SIZE)) == NULL ||
			(ctx->lz4_stream = LZ4_createStreamDecode()) == NULL)
			free_decomp_ctx_lz4(ctx);
		else success = true;
	}
#endif

#ifdef USE_ZSTD
	if (pg_streampack_compression_configured[COMPRESS_ZSTD] > 0)
	{
		if ((ctx->zstd_stream = ZSTD_createDStream()) == NULL ||
			ZSTD_isError(ZSTD_initDStream(ctx->zstd_stream)))
			free_decomp_ctx_zstd(ctx);
		else success = true;
	}
#endif

	if (!success)
		goto cleanup;

	if ((ctx->buf = createPQExpBuffer()) == NULL)
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
	decompression_ctx *ctx = pg_streampack_enabled() ? init_decomp_ctx() : NULL;

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
		char *compression_request = ctx == NULL ? "" : pg_streampack_compression_request();

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
			if (ctx == NULL)
			{
				keys[++i] = "options";
				vals[i] = LOGICAL_OPTIONS;
			}
			else
				conn_options = LOGICAL_OPTIONS;
		}
		else
		{
			/*
			 * The database name is ignored by the server in replication mode,
			 * but specify "replication" for .pgpass lookup.
			 */
			keys[++i] = "dbname";
			vals[i] = "replication";
		}

		if (ctx != NULL)
		{
			options = palloc0(1 + strlen(COMPRESSION_OPTION) +
							  strlen(compression_request) +
							  (conn_options ? strlen(conn_options) + 1 : 0));
			if (conn_options)
			{
				strcpy(options, conn_options);
				strcat(options, " ");
			}
			strcat(options, COMPRESSION_OPTION);
			strcat(options, compression_request);

			keys[++i] = "options";
			vals[i] = options;
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
	bool ret = old_walrcv_startstreaming(conn, options);

	if (ret)
		conn->streaming = conn->decomp_ctx != NULL;

	return ret;
}

static void
libpqrcv_endstreaming(WalReceiverConn *conn, TimeLineID *next_tli)
{
	conn->streaming = false;

	old_walrcv_endstreaming(conn, next_tli);
}

#ifdef USE_LZ4
static void
pg_streampack_decompress_lz4(struct decompression_ctx *ctx, uint32 expected_len,
							 const char *payload, uint32 payload_len)
{
	uint32 dict_size;
	PQExpBufferData *buf = ctx->buf;
	int decompressed_len =
		LZ4_decompress_safe_continue(ctx->lz4_stream,
									 payload,
									 &buf->data[buf->len],
									 payload_len, expected_len);
	if (decompressed_len != expected_len)
		ereport(ERROR,
				(errmsg("Failed to decompress LZ4 replication message"),
				 errhint("payload_len = %u, expected_len = %u, got = %d",
						 payload_len, expected_len, decompressed_len)));

	buf->len += decompressed_len;
	dict_size = Min(decompressed_len, LZ4_DICT_SIZE);
	memcpy(ctx->lz4_dict, &buf->data[buf->len - dict_size], dict_size);
	LZ4_setStreamDecode(ctx->lz4_stream, ctx->lz4_dict, dict_size);
}
#endif

#ifdef USE_ZSTD
static void
pg_streampack_decompress_zstd(struct decompression_ctx *ctx, uint32 expected_len,
							  const char *payload, uint32 payload_len)
{
	PQExpBufferData *buf = ctx->buf;

	ZSTD_inBuffer input = {payload, payload_len, 0};
	ZSTD_outBuffer output = {&buf->data[buf->len], expected_len, 0};

	size_t result = ZSTD_decompressStream(ctx->zstd_stream, &output, &input);

	if (ZSTD_isError(result))
		ereport(ERROR,
				(errmsg("Failed to decompress ZSTD replication message: %s",
						ZSTD_getErrorName(result)),
				 errhint("payload_len = %u, expected_len = %u, got = %lu",
						 payload_len, expected_len, output.pos)));

	buf->len += output.pos;
}
#endif

static void
maybe_set_decompress(decompression_ctx *ctx, uint32 algo)
{
	/*
	 * We set decompress only once and don't check if algo changed over
	 * runtime. If there is a mismatch decompression will fail and produce
	 * meaningful error.
	 */
	if (ctx->decompress == NULL)
	{
		switch (algo)
		{
			case COMPRESS_ZSTD:
#ifdef USE_ZSTD
				if (ctx->zstd_stream)
				{
					free_decomp_ctx_lz4(ctx);
					ctx->decompress = pg_streampack_decompress_zstd;
					elog(LOG_SERVER_ONLY, "Receiving ZSTD compressed stream");
				}
				else
#endif
					ereport(ERROR,
							(errmsg("Cannot decompress ZSTD replication message")));
				break;
			case COMPRESS_LZ4:
#ifdef USE_LZ4
				if (ctx->lz4_stream)
				{
					free_decomp_ctx_zstd(ctx);
					ctx->decompress = pg_streampack_decompress_lz4;
					elog(LOG_SERVER_ONLY, "Receiving LZ4 compressed stream");
				}
				else
#endif
					ereport(ERROR,
							(errmsg("Cannot decompress LZ4 replication message")));
				break;
			default:
				ereport(ERROR,
						(errmsg("Unknown algo %u", algo)));
		}
	}
}

static int
libpqrcv_receive(WalReceiverConn *conn, char **buffer,
				 pgsocket *wait_fd)
{
	int len = old_walrcv_receive(conn, buffer, wait_fd);

	/* Most significant bit in timestamp indicates compression. */
	if (conn->streaming && len >= new_header_size && buffer[0][0] == 'w' &&
		(uint8) (buffer[0][ts_offset] & PG_STREAMPACK_FLAG_MASK))
	{
		uint32 expected_len, algo;
		uint32 payload_len = len - new_header_size;
		decompression_ctx *ctx = conn->decomp_ctx;
		PQExpBufferData *buf = ctx->buf;

		/* get uncompressed size of payload from appended 4-byte field */
		memcpy(&expected_len, &buffer[0][header_size], sizeof(uint32));
		expected_len = pg_ntoh32(expected_len);

		/* extract algorithm from two high bits */
		algo = (expected_len >> COMPRESSION_ALGO_SHIFT) + 1;

		maybe_set_decompress(ctx, algo);

		expected_len &= ((1 << COMPRESSION_ALGO_SHIFT) - 1);

		/* Calculate length of uncompressed message. */
		len = header_size + expected_len;

		resetPQExpBuffer(buf);
		enlargePQExpBuffer(buf, len);
		if (PQExpBufferDataBroken(*buf))
			ereport(ERROR, (errmsg("Failed to allocate %u bytes", len)));

		/* copy original header */
		appendBinaryPQExpBuffer(buf, *buffer, header_size);
		/* Reset bit indicating compression */
		((uint8 *) buf->data)[ts_offset] &= (uint8) ~PG_STREAMPACK_FLAG_MASK;

		ctx->decompress(ctx, expected_len, &buffer[0][new_header_size], payload_len);

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

#if PG_VERSION_NUM < 160000
static void *
guc_malloc(int elevel, size_t size)
{
	void	   *data;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	data = malloc(size);
	if (data == NULL)
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}
#endif

static bool
compression_validate_parameter(const char *name, const char *value, void **extra)
{
	ListCell *l;
	List *elemlist;
	bool ret = false;
	char *rawstring = pstrdup(value);

	if (SplitIdentifierString(rawstring, ',', &elemlist))
	{
		uint32 seen[COMPRESS_MAX_ID] = {0,};
		uint32 order[COMPRESS_MAX_ID] = {0,};
		size_t num = 0;

		foreach(l, elemlist)
		{
			char *item = lfirst(l);

			for (size_t i = 0; i < lengthof(algo_options); i++)
			{
				struct config_enum_entry option = algo_options[i];

				if (pg_strcasecmp(item, option.name) == 0 &&
					seen[option.val - 1] == 0)
				{
					order[num] = option.val;
					seen[option.val - 1] = ++num;
					goto next;
				}
			}

			GUC_check_errdetail("Invalid option \"%s\".", item);
			goto error;
next:;
		}

		if ((*extra = guc_malloc(LOG, COMPRESS_MAX_ID * sizeof(uint32))) != NULL)
		{
			memcpy(*extra, order, COMPRESS_MAX_ID * sizeof(uint32));
			ret = true;
		}
	}
	else
		GUC_check_errdetail("Invalid list syntax in parameter \"%s\".", name);

error:
	pfree(rawstring);
	list_free(elemlist);
	return ret;
}

static bool
compression_check_hook(char **newval, void **extra, GucSource source)
{
	return compression_validate_parameter("pg_streampack.compression", *newval, extra);
}

static void
compression_assign_hook(const char *newval, void *extra)
{
	uint32 *enabled = extra;

	memcpy(pg_streampack_compression_order, enabled, COMPRESS_MAX_ID * sizeof(uint32));
	memset(pg_streampack_compression_configured, 0, (COMPRESS_MAX_ID + 1) * sizeof(bool));

	for (size_t i = 0; i < COMPRESS_MAX_ID && enabled[i] > 0; i++)
		pg_streampack_compression_configured[enabled[i]] = true;
}

static bool
compression_requested_check_hook(char **newval, void **extra, GucSource source)
{
	/* allow setting pg_streampack.requested only from client connection */
	return (source == PGC_S_DEFAULT || source == PGC_S_CLIENT) &&
		compression_validate_parameter("pg_streampack.requested", *newval, extra);
}

static void
compression_requested_assign_hook(const char *newval, void *extra)
{
	uint32 *requested = extra;

	if (comp_ctx != NULL)
	{
		for (size_t i = 0; i < COMPRESS_MAX_ID && requested[i] > 0; i++)
			if (pg_streampack_compression_configured[requested[i]])
			{
				switch (requested[i])
				{
					case COMPRESS_ZSTD:
#ifdef USE_ZSTD
						if (comp_ctx->zstd_stream != NULL)
						{
							free_comp_ctx_lz4(comp_ctx);
							comp_ctx->compress = pg_streampack_compress_zstd;
							elog(LOG_SERVER_ONLY, "Sending ZSTD compressed stream");
						}
#endif
						break;
					case COMPRESS_LZ4:
#ifdef USE_LZ4
						if (comp_ctx->lz4_stream != NULL)
						{
							free_comp_ctx_ztsd(comp_ctx);
							comp_ctx->compress = pg_streampack_compress_lz4;
							elog(LOG_SERVER_ONLY, "Sending LZ4 compressed stream");
						}
#endif
						break;
				}
			}

		if (comp_ctx->compress == NULL)
		{
			free_comp_ctx(comp_ctx);
			comp_ctx = NULL;
		}
	}
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
		 "pg_streampack state: total: " UINT64_FORMAT " bytes, uncompressed: " UINT64_FORMAT " bytes, compressed: " UINT64_FORMAT " bytes, compression increased size: " UINT64_FORMAT " times",
		 (uint64) pg_atomic_read_u64(&stats->total),
		 (uint64) pg_atomic_read_u64(&stats->uncompressed),
		 (uint64) pg_atomic_read_u64(&stats->compressed),
		 (uint64) pg_atomic_read_u64(&stats->increased));
}

static void
shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	stats = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	stats = ShmemInitStruct("pg_streampack", memsize(), &found);
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

	DefineCustomStringVariable("pg_streampack.compression",
							   "Comma separated list of compression algorithms.",
							   NULL,
							   &pg_streampack_compression_string,
							   "",
							   PGC_SIGHUP,
							   GUC_LIST_INPUT,
							   compression_check_hook,
							   compression_assign_hook,
							   NULL);

	DefineCustomStringVariable("pg_streampack.requested",
							   "Comma separated list of compression algorithms requested by client and ordered by preference.",
							   NULL,
							   &pg_streampack_client_requested_string,
							   "",
							   PGC_BACKEND,
							   GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE | GUC_NO_SHOW_ALL,
							   compression_requested_check_hook,
							   compression_requested_assign_hook,
							   NULL);

	DefineCustomIntVariable("pg_streampack.min_size",
							"Minimal size of payload to compress.",
							"",
							&min_compress_size,
							32,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);
	EmitWarningsOnPlaceholders("pg_streampack");
}
