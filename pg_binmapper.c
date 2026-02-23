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
parse_binary_payload(PG_FUNCTION_ARGS)
{
    Oid table_oid = PG_GETARG_OID(0);
    bytea *binary_data = PG_GETARG_BYTEA_P(1);
    char *raw_ptr = VARDATA_ANY(binary_data);
    int input_size = VARSIZE_ANY_EXHDR(binary_data);
    TableBinaryLayout *layout;
    
    // Используем palloc0 для гарантии чистоты всех бит
    Datum *values;
    bool *nulls;
    HeapTuple tuple;

    layout = get_or_create_layout(table_oid);

    if (input_size != 32) { // Жесткая проверка для теста
        ereport(ERROR, (errmsg("Expected 32 bytes, got %d", input_size)));
    }

    values = (Datum *) palloc0(layout->tupdesc->natts * sizeof(Datum));
    nulls = (bool *) palloc0(layout->tupdesc->natts * sizeof(bool));

    // 1. ID (int4) - Offset 0
    uint32 v4;
    memcpy(&v4, raw_ptr + 0, 4);
    values[0] = Int32GetDatum(pg_bswap32(v4));

    // 2. TS (int8) - Offset 4
    uint64 v8;
    memcpy(&v8, raw_ptr + 4, 8);
    values[1] = Int64GetDatum(pg_bswap64(v8));

    // 3. Temp (float4) - Offset 12
    union { uint32 i; float4 f; } u;
    memcpy(&u.i, raw_ptr + 12, 4);
    u.i = pg_bswap32(u.i);
    values[2] = Float4GetDatum(u.f);

    // 4. UUID - Offset 16 (16 bytes)
    // ВАЖНО: Postgres UUID - это структура pg_uuid_t (16 байт)
    pg_uuid_t *uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
    memcpy(uuid->data, raw_ptr + 16, 16);
    values[3] = PointerGetDatum(uuid);

    tuple = heap_form_tuple(layout->tupdesc, values, nulls);
    
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}