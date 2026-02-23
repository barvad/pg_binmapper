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
    TableBinaryLayout * layout = (TableBinaryLayout * ) hash_search(layout_cache, & relid, HASH_ENTER, & found);

    if (!found || !layout -> is_valid) {
      Relation rel = table_open(relid, AccessShareLock);
      TupleDesc res_tupdesc = RelationGetDescr(rel);
      int natts = res_tupdesc -> natts;

      layout -> tupdesc = CreateTupleDescCopy(res_tupdesc);
      layout -> offsets = (int * ) MemoryContextAlloc(TopMemoryContext, natts * sizeof(int));
      layout -> total_binary_size = 0;

      for (int i = 0; i < natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(layout -> tupdesc, i);
        if (attr -> attisdropped) continue;

        if (attr -> attlen < 0) {
          table_close(rel, AccessShareLock);
          ereport(ERROR, (errmsg("Колонка %s имеет переменную длину. Поддерживаются только фиксированные типы.", NameStr(attr -> attname))));
        }

        layout -> offsets[i] = layout -> total_binary_size;
        layout -> total_binary_size += attr -> attlen;
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
    int len = 0;

    if (attr -> attisdropped) {
      nulls[i] = true;
      continue;
    }

    // определяем реальную длину типа
    if (attr -> attlen > 0) {
      len = attr -> attlen;
    } else if (attr -> atttypid == 2950) { // UUID OID
      len = 16;
    } else {
      ereport(ERROR, (errmsg("Unsupported type OID %u for column %s",
        attr -> atttypid, NameStr(attr -> attname))));
    }

    if (attr -> attbyval) {
      Datum val = 0;
      memcpy( & val, field_ptr, (len <= sizeof(Datum)) ? len : sizeof(Datum));

      // Разворот Big-Endian
      if (len == 8) val = pg_bswap64(val);
      else if (len == 4) val = (Datum) pg_bswap32((uint32) val);
      else if (len == 2) val = (Datum) pg_bswap16((uint16) val);

      values[i] = val;
    } else {
      // Ссылочные типы (UUID и т.д.)
      void * copy = palloc(len);
      memcpy(copy, field_ptr, len);
      values[i] = PointerGetDatum(copy);
    }
  }

  HeapTuple tuple = heap_form_tuple(layout -> tupdesc, values, nulls);
  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}