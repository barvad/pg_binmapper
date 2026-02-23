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

#include "utils/uuid.h"

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

static TableBinaryLayout*
get_or_create_layout(Oid relid) {
    bool found;
    TableBinaryLayout *layout = (TableBinaryLayout *) hash_search(layout_cache, &relid, HASH_ENTER, &found);

    if (!found || !layout->is_valid) {
        Relation rel = table_open(relid, AccessShareLock);
        TupleDesc res_tupdesc = RelationGetDescr(rel);
        int natts = res_tupdesc->natts;

        layout->tupdesc = CreateTupleDescCopy(res_tupdesc);
        layout->offsets = (int *) MemoryContextAllocZero(TopMemoryContext, natts * sizeof(int));
        layout->total_binary_size = 0;

        /* Инициализируем оффсеты как -1 (пропуск) */
        for (int i = 0; i < natts; i++) layout->offsets[i] = -1;

        /* ВАЖНО: Идем по порядку номеров колонок (1, 2, 3...) */
        for (int i = 0; i < natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(layout->tupdesc, i);

            /* Пропускаем удаленные и системные колонки */
            if (attr->attisdropped || attr->attnum <= 0) continue;

            int col_len = 0;
            if (attr->attlen > 0) col_len = attr->attlen;
            else if (attr->atttypid == 2950) col_len = 16; /* UUID */
            else continue;

            layout->offsets[i] = layout->total_binary_size;
            layout->total_binary_size += col_len;
        }

        layout->is_valid = true;
        table_close(rel, AccessShareLock);
    }
    return layout;
}

PG_FUNCTION_INFO_V1(parse_binary_payload);

Datum
parse_binary_payload(PG_FUNCTION_ARGS)
{
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
    int i;

    layout = get_or_create_layout(table_oid);
    tupdesc = layout->tupdesc;

    if (data_len != layout->total_binary_size)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("Expected %d bytes, got %d", layout->total_binary_size, data_len)));
    }

    values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
    nulls = (bool *) palloc0(tupdesc->natts * sizeof(bool));

    for (int i = 0; i < layout->tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(layout->tupdesc, i);
        
        if (layout->offsets[i] == -1) {
            nulls[i] = true;
            continue;
        }

        char *field_ptr = raw_ptr + layout->offsets[i];
        int len = (attr->attlen > 0) ? attr->attlen : 16;

        if (attr->attbyval) {
            uint64 raw_val = 0;
            memcpy(&raw_val, field_ptr, len);

            if (len == 8) raw_val = pg_bswap64(raw_val);
            else if (len == 4) raw_val = (uint64)pg_bswap32((uint32)raw_val);
            else if (len == 2) raw_val = (uint64)pg_bswap16((uint16)raw_val);

            /* Используем макросы для float4 */
            if (attr->atttypid == 700) {
                union { uint32 i; float4 f; } u;
                u.i = (uint32)raw_val;
                values[i] = Float4GetDatum(u.f);
            } else {
                values[i] = (Datum)raw_val;
            }
        } else {
            /* UUID - создаем копию и передаем УКАЗАТЕЛЬ */
            void *copy = palloc(len);
            memcpy(copy, field_ptr, len);
            values[i] = PointerGetDatum(copy);
        }
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);
    
    pfree(values);
    pfree(nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}