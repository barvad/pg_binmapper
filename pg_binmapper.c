#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "port/pg_bswap.h"
#include "utils/uuid.h"

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
    if (layout_cache) {
        hash_search(layout_cache, &relid, HASH_REMOVE, NULL);
    }
}

void _PG_init(void) {
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(TableBinaryLayout);
    layout_cache = hash_create("TableBinaryLayoutCache", 1024, &ctl, HASH_ELEM | HASH_BLOBS);
    CacheRegisterRelcacheCallback(invalidate_layout_cache, (Datum) 0);
}

static TableBinaryLayout* get_or_create_layout(Oid relid) {
    bool found;
    TableBinaryLayout *layout = (TableBinaryLayout *) hash_search(layout_cache, &relid, HASH_ENTER, &found);

    if (!found || !layout->is_valid) {
        Relation rel = table_open(relid, AccessShareLock);
        TupleDesc res_tupdesc = RelationGetDescr(rel);
        int natts = res_tupdesc->natts;
        int i;

        layout->tupdesc = CreateTupleDescCopy(res_tupdesc);
        layout->offsets = (int *) MemoryContextAllocZero(TopMemoryContext, natts * sizeof(int));
        layout->total_binary_size = 0;

        for (i = 0; i < natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(layout->tupdesc, i);
            int col_len = 0;

            if (attr->attisdropped || attr->attnum <= 0) {
                layout->offsets[i] = -1;
                continue;
            }

            if (attr->attlen > 0) {
                col_len = attr->attlen;
            } else if (attr->atttypid == 2950) { /* UUID OID */
                col_len = 16;
            } else {
                table_close(rel, AccessShareLock);
                ereport(ERROR, (errmsg("Column %s has unsupported var-length type", NameStr(attr->attname))));
            }

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
    char *raw_ptr = VARDATA_ANY(payload);
    int data_len = VARSIZE_ANY_EXHDR(payload);

    TableBinaryLayout *layout;
    Datum *values;
    bool *nulls;
    HeapTuple tuple;
    int i;

    elog(LOG, "[BINMAPPER] Start parsing for Table OID: %u, Payload len: %d", table_oid, data_len);

    layout = get_or_create_layout(table_oid);

    if (data_len != layout->total_binary_size) {
        elog(ERROR, "[BINMAPPER] Size mismatch! Table expects %d, but got %d", layout->total_binary_size, data_len);
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

        elog(LOG, "[BINMAPPER] Processing col %d: %s (TypeOID: %u, Offset: %d, Len: %d)", 
             i, NameStr(attr->attname), attr->atttypid, layout->offsets[i], len);

        if (attr->attbyval) {
            uint64 tmp = 0;
            memcpy(&tmp, field_ptr, (len <= 8) ? len : 8);

            if (len == 8) tmp = pg_bswap64(tmp);
            else if (len == 4) tmp = (uint64)pg_bswap32((uint32)tmp);
            else if (len == 2) tmp = (uint64)pg_bswap16((uint16)tmp);

            if (attr->atttypid == FLOAT4OID) {
                union { uint32 i; float4 f; } u;
                u.i = (uint32)tmp;
                values[i] = Float4GetDatum(u.f);
                elog(LOG, "[BINMAPPER] Float4 value: %f", u.f);
            } else {
                values[i] = (Datum)tmp;
                elog(LOG, "[BINMAPPER] Int/ByVal value: %ld", (long)tmp);
            }
        } else {
            if (attr->atttypid == UUIDOID) {
                pg_uuid_t *uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
                memcpy(uuid->data, field_ptr, 16);
                values[i] = UUIDPGetDatum(uuid);
                elog(LOG, "[BINMAPPER] UUID Pointer: %p", uuid);
            } else {
                void *copy = palloc(len);
                memcpy(copy, field_ptr, len);
                values[i] = PointerGetDatum(copy);
                elog(LOG, "[BINMAPPER] Other ByRef Pointer: %p", copy);
            }
        }
    }

    elog(LOG, "[BINMAPPER] All fields mapped. Calling heap_form_tuple...");
    tuple = heap_form_tuple(layout->tupdesc, values, nulls);
    elog(LOG, "[BINMAPPER] Tuple formed successfully.");

    HeapTupleHeader res = (HeapTupleHeader) palloc(tuple->t_len);
    memcpy(res, tuple->t_header, tuple->t_len);
    
    // Устанавливаем метаданные типа, чтобы Postgres опознал структуру
    HeapTupleHeaderSetTypeId(res, table_oid);
    HeapTupleHeaderSetTypMod(res, -1);

    pfree(values);
    nulls ? pfree(nulls) : 0;
    heap_freetuple(tuple);

    PG_RETURN_HEAPTUPLEHEADER_DATUM(res);
}
