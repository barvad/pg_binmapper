#include "postgres.h"

#include "fmgr.h"

#include "access/table.h"

#include "utils/rel.h"

#include "utils/lsyscache.h"

#include "catalog/pg_type.h"

#include "funcapi.h"

#include "utils/inval.h"

#include "utils/hsearch.h"

#include "port/pg_bswap.h"

#include "access/htup_details.h"

PG_MODULE_MAGIC;

typedef struct {
  Oid relid;
  TupleDesc tupdesc;
  int * offsets;
  int total_binary_size;
  bool is_valid;
}
TableBinaryLayout;

static HTAB * layout_cache = NULL;

static void
invalidate_layout_cache(Datum arg, Oid relid) {
  if (layout_cache) {
    hash_search(layout_cache, & relid, HASH_REMOVE, NULL);
  }
}

void _PG_init(void) {
  HASHCTL ctl;
  memset( & ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(Oid);
  ctl.entrysize = sizeof(TableBinaryLayout);
  layout_cache = hash_create("TableBinaryLayoutCache", 1024, & ctl, HASH_ELEM | HASH_BLOBS);
  CacheRegisterRelcacheCallback(invalidate_layout_cache, (Datum) 0);
}

static TableBinaryLayout *
  get_or_create_layout(Oid relid) {
    bool found;
    /* Ищем или создаем запись в кэше */
    TableBinaryLayout * layout = (TableBinaryLayout * ) hash_search(layout_cache, & relid, HASH_ENTER, & found);

    if (!found || !layout -> is_valid) {
      Relation rel = table_open(relid, AccessShareLock);
      TupleDesc res_tupdesc = RelationGetDescr(rel);
      int natts = res_tupdesc -> natts;

      /* Инициализируем поля структуры, чтобы там не было мусора */
      layout -> relid = relid;
      layout -> total_binary_size = 0;
      layout -> tupdesc = CreateTupleDescCopy(res_tupdesc);

      /* Выделяем память под оффсеты в долгоживущем контексте */
      layout -> offsets = (int * ) MemoryContextAllocZero(TopMemoryContext, natts * sizeof(int));

      for (int i = 0; i < natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(layout -> tupdesc, i);
        int col_len = 0;

        if (attr -> attisdropped) continue;

        if (attr -> attlen > 0) {
          col_len = attr -> attlen;
        } else if (attr -> atttypid == 2950) { // UUID
          col_len = 16;
        } else {
          table_close(rel, AccessShareLock);
          ereport(ERROR, (errmsg("Unsupported type for column %s", NameStr(attr -> attname))));
        }

        layout -> offsets[i] = layout -> total_binary_size;
        layout -> total_binary_size += col_len;
      }

      layout -> is_valid = true;
      table_close(rel, AccessShareLock);
    }
    return layout;
  }

PG_FUNCTION_INFO_V1(parse_binary_payload);

Datum
parse_binary_payload(PG_FUNCTION_ARGS) {
  Oid table_oid = PG_GETARG_OID(0);
  bytea * binary_data = PG_GETARG_BYTEA_P(1);
  char * raw_ptr = VARDATA_ANY(binary_data);
  int input_size = VARSIZE_ANY_EXHDR(binary_data);

  TableBinaryLayout * layout = get_or_create_layout(table_oid);

  if (input_size != layout -> total_binary_size) {
    ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
      errmsg("Binary size mismatch: expected %d, got %d", layout -> total_binary_size, input_size)));
  }

  Datum * values = palloc(layout -> tupdesc -> natts * sizeof(Datum));
  bool * nulls = palloc0(layout -> tupdesc -> natts * sizeof(bool));

  for (int i = 0; i < layout -> tupdesc -> natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(layout -> tupdesc, i);
    char * field_ptr = raw_ptr + layout -> offsets[i];
    int len = (attr -> attlen > 0) ? attr -> attlen : (attr -> atttypid == 2950 ? 16 : 0);

    if (attr -> attisdropped) {
      nulls[i] = true;
      continue;
    }
    if (len <= 0) ereport(ERROR, (errmsg("Unsupported type size for %s", NameStr(attr -> attname))));

    if (attr -> attbyval) {
      if (len == 8) {
        uint64 val8;
        memcpy( & val8, field_ptr, 8);
        values[i] = Int64GetDatum(pg_bswap64(val8));
      } else if (len == 4) {
        uint32 val4;
        memcpy( & val4, field_ptr, 4);
        values[i] = Int32GetDatum(pg_bswap32(val4));
      } else if (len == 2) {
        uint16 val2;
        memcpy( & val2, field_ptr, 2);
        values[i] = Int16GetDatum(pg_bswap16(val2));
      } else {
        values[i] = (Datum) 0;
        memcpy( & values[i], field_ptr, len);
      }
    } else {
      // Для UUID и других ссылочных типов (palloc безопасен)
      void * copy = palloc(len);
      memcpy(copy, field_ptr, len);
      values[i] = PointerGetDatum(copy);
    }
  }

  HeapTuple tuple = heap_form_tuple(layout -> tupdesc, values, nulls);
  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}