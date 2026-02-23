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
    char *data = VARDATA_ANY(payload);
    int data_len = VARSIZE_ANY_EXHDR(payload);

    TableBinaryLayout *layout;
    TupleDesc tupdesc;
    AttInMetadata *attinmeta;
    char **values_str;
    HeapTuple tuple;
    int i;

    layout = get_or_create_layout(table_oid);
    tupdesc = layout->tupdesc;

    if (data_len != layout->total_binary_size)
        elog(ERROR, "Size mismatch: expected %d, got %d", layout->total_binary_size, data_len);

    /* Используем AttInMetadata для безопасной сборки кортежа */
    attinmeta = TupleDescGetAttInMetadata(tupdesc);
    values_str = (char **) palloc(tupdesc->natts * sizeof(char *));

    for (i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (layout->offsets[i] == -1) { values_str[i] = NULL; continue; }

        char *ptr = data + layout->offsets[i];
        
        if (attr->atttypid == INT4OID) {
            uint32 v; memcpy(&v, ptr, 4);
            values_str[i] = psprintf("%d", (int)pg_bswap32(v));
        } else if (attr->atttypid == INT8OID) {
            uint64 v; memcpy(&v, ptr, 8);
            values_str[i] = psprintf("%ld", (long)pg_bswap64(v));
        } else if (attr->atttypid == FLOAT4OID) {
            union { uint32 i; float4 f; } u;
            memcpy(&u.i, ptr, 4); u.i = pg_bswap32(u.i);
            values_str[i] = psprintf("%g", u.f);
        } else if (attr->atttypid == UUIDOID) {
            /* Конвертируем бинарный UUID в строку для безопасной вставки */
            values_str[i] = palloc(37);
            snprintf(values_str[i], 37, 
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                (unsigned char)ptr[0], (unsigned char)ptr[1], (unsigned char)ptr[2], (unsigned char)ptr[3],
                (unsigned char)ptr[4], (unsigned char)ptr[5], (unsigned char)ptr[6], (unsigned char)ptr[7],
                (unsigned char)ptr[8], (unsigned char)ptr[9], (unsigned char)ptr[10], (unsigned char)ptr[11],
                (unsigned char)ptr[12], (unsigned char)ptr[13], (unsigned char)ptr[14], (unsigned char)ptr[15]);
        }
    }

    /* Postgres сам создаст правильный HeapTupleHeader с нужным выравниванием */
    tuple = BuildTupleFromCStrings(attinmeta, values_str);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}