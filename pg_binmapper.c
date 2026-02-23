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


Datum parse_binary_payload(PG_FUNCTION_ARGS) {
    Oid table_oid = PG_GETARG_OID(0);
    bytea *payload = PG_GETARG_BYTEA_P(1);
    char *data = VARDATA_ANY(payload);
    int data_len = VARSIZE_ANY_EXHDR(payload);

    TableBinaryLayout *layout;
    TupleDesc tupdesc;
    Datum *values;
    bool *nulls;
    int offset = 0;
    HeapTuple tuple; 

    layout = get_or_create_layout(table_oid);
    tupdesc = layout->tupdesc;
    
    values = palloc0(tupdesc->natts * sizeof(Datum));
    nulls = palloc0(tupdesc->natts * sizeof(bool));

    if (data_len != layout->total_binary_size)
        elog(ERROR, "Expected %d bytes, got %d", layout->total_binary_size, data_len);

    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (attr->attisdropped) { nulls[i] = true; continue; }

        if (attr->attbyval) {
            // Читаем числовые типы (int, float)
            uint64 tmp = 0;
            memcpy(&tmp, data + offset, attr->attlen);

            // Разворот Big-Endian
            if (attr->attlen == 8) tmp = pg_bswap64(tmp);
            else if (attr->attlen == 4) tmp = pg_bswap32((uint32)tmp);
            else if (attr->attlen == 2) tmp = pg_bswap16((uint16)tmp);

            // Магия Float4: Postgres хранит float4 в Datum как "притворяющийся" int32
            if (attr->atttypid == FLOAT4OID) {
                union { uint32 i; float4 f; } u;
                u.i = (uint32)tmp;
                values[i] = Float4GetDatum(u.f);
            } else {
                values[i] = (Datum)tmp;
            }
        } else {
            // Для ссылочных типов (UUID) копируем данные в новую память
            // Postgres сам позаботится о выравнивании внутри heap_form_tuple
            int len = (attr->atttypid == 2950) ? 16 : attr->attlen;
            char *copy = palloc(len);
            memcpy(copy, data + offset, len);
            values[i] = PointerGetDatum(copy);
        }
        offset += (attr->attlen > 0) ? attr->attlen : 16;
    }

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}