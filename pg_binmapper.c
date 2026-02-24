#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/uuid.h"
#include "port/pg_bswap.h"
#include "funcapi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

typedef struct {
    Oid relid;
    TupleDesc tupdesc;
    int *offsets;
    int total_binary_size;
    bool is_valid;
} TableBinaryLayout;

static HTAB *layout_cache = NULL;

static void invalidate_layout_cache(Datum arg, Oid relid) {
    if (layout_cache) hash_search(layout_cache, &relid, HASH_REMOVE, NULL);
}

void _PG_init(void) {
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TableBinaryLayout);
    layout_cache = hash_create("BinMapperCache", 1024, &ctl, HASH_ELEM | HASH_BLOBS);
    CacheRegisterRelcacheCallback(invalidate_layout_cache, (Datum) 0);
}

static TableBinaryLayout* get_or_create_layout(Oid relid) {
    bool found;
    TableBinaryLayout *layout = (TableBinaryLayout *) hash_search(layout_cache, &relid, HASH_ENTER, &found);
    if (!found || !layout->is_valid) {
        Relation rel = table_open(relid, AccessShareLock);
        TupleDesc desc = RelationGetDescr(rel);
        int i;
        layout->tupdesc = CreateTupleDescCopy(desc);
        layout->offsets = (int *) MemoryContextAllocZero(TopMemoryContext, desc->natts * sizeof(int));
        layout->total_binary_size = 0;
        for (i = 0; i < desc->natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(layout->tupdesc, i);
            int len = 0;
            if (attr->attisdropped || attr->attnum <= 0) { layout->offsets[i] = -1; continue; }
            if (attr->attlen > 0) len = attr->attlen;
            else if (attr->atttypid == UUIDOID) len = 16;
            else { table_close(rel, AccessShareLock); elog(ERROR, "Type not supported"); }
            layout->offsets[i] = layout->total_binary_size;
            layout->total_binary_size += len;
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
    char *raw_ptr = VARDATA_ANY(payload);
    int input_size = VARSIZE_ANY_EXHDR(payload);

    TableBinaryLayout *layout;
    Datum *values;
    bool *nulls;
    HeapTuple tuple;
    Datum result_datum;
    int i;

    layout = get_or_create_layout(table_oid);

    elog(LOG, "[BINMAPPER] Start: TableOID=%u, InputSize=%d, ExpectedSize=%d", 
         table_oid, input_size, layout->total_binary_size);

    if (input_size != layout->total_binary_size) {
        ereport(ERROR, (errmsg("SIZE ERROR: expected %d, got %d", 
                layout->total_binary_size, input_size)));
    }

    values = (Datum *) palloc0(layout->tupdesc->natts * sizeof(Datum));
    nulls = (bool *) palloc0(layout->tupdesc->natts * sizeof(bool));

    for (i = 0; i < layout->tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(layout->tupdesc, i);
        char *field_ptr;
        int len;

        if (layout->offsets[i] == -1) {
            nulls[i] = true;
            continue;
        }

        field_ptr = raw_ptr + layout->offsets[i];
        len = (attr->attlen > 0) ? attr->attlen : 16;

        if (attr->attbyval) {
            uint64 raw_val = 0;
            memcpy(&raw_val, field_ptr, (len <= 8) ? len : 8);

            if (len == 8) raw_val = pg_bswap64(raw_val);
            else if (len == 4) raw_val = (uint64)pg_bswap32((uint32)raw_val);
            else if (len == 2) raw_val = (uint64)pg_bswap16((uint16)raw_val);

            if (attr->atttypid == FLOAT4OID) {
                union { uint32 i; float4 f; } u;
                u.i = (uint32)raw_val;
                values[i] = Float4GetDatum(u.f);
                elog(DEBUG1, "[BINMAPPER] Col %d (float4): %f", i, u.f);
            } else {
                values[i] = (Datum)raw_val;
                elog(DEBUG1, "[BINMAPPER] Col %d (int/long): %ld", i, (long)raw_val);
            }
        } else {
            pg_uuid_t *uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
            memcpy(uuid->data, field_ptr, 16);
            values[i] = UUIDPGetDatum(uuid);
            elog(DEBUG1, "[BINMAPPER] Col %d (uuid) mapped", i);
        }
    }

    elog(LOG, "[BINMAPPER] Forming tuple and blessing descriptor");
    
    /* 
     * BlessTupleDesc регистрирует структуру RECORD в системе.
     * Это критично для функций, возвращающих анонимные рекорды.
     */
    BlessTupleDesc(layout->tupdesc);
    tuple = heap_form_tuple(layout->tupdesc, values, nulls);
    
    /* Устанавливаем метаданные типа */
    HeapTupleHeaderSetTypeId(tuple->t_data, get_rel_type_id(table_oid));
    HeapTupleHeaderSetTypMod(tuple->t_data, -1);

    elog(LOG, "[BINMAPPER] Returning HeapTupleHeader as Datum");

    /* 
     * ВНИМАНИЕ: Мы НЕ делаем heap_freetuple(tuple) и pfree(values).
     * Postgres сам очистит MemoryContext функции после выполнения INSERT.
     * Если мы удалим это сейчас, Postgres получит битый указатель (Segmentation Fault).
     */
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
