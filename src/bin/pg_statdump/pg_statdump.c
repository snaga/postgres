/*
 * statdump
 */
#include "postgres.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <gdbm.h>

#include "pgstat.h"
#include "utils/timestamp.h"

/*
 * duplicated from src/backend/utils/adt/timestamp.c.
 */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t       result;

#ifdef HAVE_INT64_TIMESTAMP
	result = (pg_time_t) (t / USECS_PER_SEC +
						  ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
	result = (pg_time_t) (t +
						  ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif

	return result;
}


static char *
timestamptz_to_cstring(TimestampTz ts)
{
	static char buf[32];
	time_t t;

	t = timestamptz_to_time_t(ts);

	strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&t));

	return buf;
}
size_t strftime(char *s, size_t max, const char *format,
				const struct tm *tm);

int
main(int argc, char *argv[])
{
	GDBM_FILE db;
	datum key, value;
	PgStat_GlobalStats globalStats;
	PgStat_StatDBEntry statDBEntry;
	PgStat_StatTabEntry statTabEntry;
	PgStat_StatFuncEntry statFuncEntry;
	
	if ( argv[1] == NULL )
    {
		printf("Usage: pg_statdump [dbm]\n");
		return 1;
    }
	
	db = gdbm_open(argv[1], 512, GDBM_WRCREAT|GDBM_NOLOCK, S_IRUSR|S_IWUSR, NULL);
	if ( db==NULL )
	{
		printf("dbm_open error %d\n", gdbm_errno);
		return 1;
    }
	
	key = gdbm_firstkey(db);
	for (key = gdbm_firstkey(db) ; key.dptr!=NULL ; key = gdbm_nextkey(db, key))
    {
		Oid oid;
		
		value = gdbm_fetch(db, key);
		if (value.dptr == NULL)
		{
			continue;
		}
		
		memcpy(&oid, key.dptr, key.dsize);
//		printf("oid=%d, type=%c, size=%d\n", oid, value.dptr[0], value.dsize-1);

		if (value.dptr[0]== 'G')
		{
			memcpy(&globalStats, value.dptr+1, value.dsize-1);
			printf("PgStat_GlobalStats : { ");
			printf("\"%s\",", timestamptz_to_cstring(globalStats.stats_timestamp));
			printf("%ld,", globalStats.timed_checkpoints);
			printf("%ld,", globalStats.requested_checkpoints);
			printf("%ld,", globalStats.checkpoint_write_time);
			printf("%ld,", globalStats.checkpoint_sync_time);
			printf("%ld,", globalStats.buf_written_checkpoints);
			printf("%ld,", globalStats.buf_written_clean);
			printf("%ld,", globalStats.maxwritten_clean);
			printf("%ld,", globalStats.buf_written_backend);
			printf("%ld,", globalStats.buf_fsync_backend);
			printf("%ld,", globalStats.buf_alloc);
			printf("\"%s\"", timestamptz_to_cstring(globalStats.stat_reset_timestamp));
			printf(" }\n");
		}
		if (value.dptr[0]== 'D')
		{
			memcpy(&statDBEntry, value.dptr+1, value.dsize-1);
			printf("PgStat_StatDBEntry : { ");
			printf("%d,", statDBEntry.databaseid);
			printf("%ld,", statDBEntry.n_xact_commit);
			printf("%ld,", statDBEntry.n_xact_rollback);
			printf("%ld,", statDBEntry.n_blocks_fetched);
			printf("%ld,", statDBEntry.n_blocks_hit);
			printf("%ld,", statDBEntry.n_tuples_returned);
			printf("%ld,", statDBEntry.n_tuples_fetched);
			printf("%ld,", statDBEntry.n_tuples_inserted);
			printf("%ld,", statDBEntry.n_tuples_updated);
			printf("%ld,", statDBEntry.n_tuples_deleted);
			printf("\"%s\",", timestamptz_to_cstring(statDBEntry.last_autovac_time));
			printf("%ld,", statDBEntry.n_conflict_tablespace);
			printf("%ld,", statDBEntry.n_conflict_lock);
			printf("%ld,", statDBEntry.n_conflict_snapshot);
			printf("%ld,", statDBEntry.n_conflict_bufferpin);
			printf("%ld,", statDBEntry.n_conflict_startup_deadlock);
			printf("%ld,", statDBEntry.n_temp_files);
			printf("%ld,", statDBEntry.n_temp_bytes);
			printf("%ld,", statDBEntry.n_deadlocks);
			printf("%ld,", statDBEntry.n_block_read_time);
			printf("%ld,", statDBEntry.n_block_write_time);
			printf("\"%s\",", timestamptz_to_cstring(statDBEntry.stat_reset_timestamp));
			printf("\"%s\"", timestamptz_to_cstring(statDBEntry.stats_timestamp));
			printf(" }\n");
		}
		if (value.dptr[0]== 'T')
		{
			memcpy(&statTabEntry, value.dptr+1, value.dsize-1);
			printf("PgStat_StatTabEntry : { ");
			printf("%d,", statTabEntry.tableid);
			printf("%ld,", statTabEntry.numscans);
			printf("%ld,", statTabEntry.tuples_returned);
			printf("%ld,", statTabEntry.tuples_fetched);
			printf("%ld,", statTabEntry.tuples_inserted);
			printf("%ld,", statTabEntry.tuples_updated);
			printf("%ld,", statTabEntry.tuples_deleted);
			printf("%ld,", statTabEntry.tuples_hot_updated);
			printf("%ld,", statTabEntry.n_live_tuples);
			printf("%ld,", statTabEntry.n_dead_tuples);
			printf("%ld,", statTabEntry.changes_since_analyze);
			printf("%ld,", statTabEntry.blocks_fetched);
			printf("%ld,", statTabEntry.blocks_hit);
			printf("\"%s\",", timestamptz_to_cstring(statTabEntry.vacuum_timestamp));
			printf("%ld,", statTabEntry.vacuum_count);
			printf("\"%s\",", timestamptz_to_cstring(statTabEntry.autovac_vacuum_timestamp));
			printf("%ld,", statTabEntry.autovac_vacuum_count);
			printf("\"%s\",", timestamptz_to_cstring(statTabEntry.analyze_timestamp));
			printf("%ld,", statTabEntry.analyze_count);
			printf("\"%s\",", timestamptz_to_cstring(statTabEntry.autovac_analyze_timestamp));
			printf("%ld", statTabEntry.autovac_analyze_count);
			printf(" }\n");
		}
		if (value.dptr[0]== 'F')
		{
			memcpy(&statFuncEntry, value.dptr+1, value.dsize-1);
			printf("PgStat_StatFuncEntry : { ");
			printf("%d,", statFuncEntry.functionid);
			printf("%ld,", statFuncEntry.f_numcalls);
			printf("%ld,", statFuncEntry.f_total_time);
			printf("%ld", statFuncEntry.f_self_time);
			printf(" }\n");
		}
    }
	
	gdbm_close(db);
	
	return 0;
}
