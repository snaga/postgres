/*
 * statdump
 */
#include "postgres.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <gdbm.h>

#include "pgstat.h"

int
main(int argc, char *argv[])
{
  GDBM_FILE db;
  datum key, value;
  
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
      
      memcpy(&oid, key.dptr, key.dsize);
      printf("oid=%d, type=%c, size=%d\n", oid, value.dptr[0], value.dsize-1);
    }
  
  gdbm_close(db);

  return 0;
}
