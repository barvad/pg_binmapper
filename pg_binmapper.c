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
    Datum *values;
    bool *nulls;
    HeapTuple tuple;

    layout = get_or_create_layout(table_oid);

    // СТРОГАЯ ПРОВЕРКА РАЗМЕРА (чтобы не было смещений)
    if (input_size != layout->total_binary_size) {
        ereport(ERROR, (errmsg("SIZE ERROR: expected %d, got %d", 
                layout->total_binary_size, input_size)));
    }

    values = (Datum *) palloc0(layout->tupdesc->natts * sizeof(Datum));
    nulls = (bool *) palloc0(layout->tupdesc->natts * sizeof(bool));

    {
        uint32 val4;
        memcpy(&val4, raw_ptr + 0, 4);
        values[0] = Int32GetDatum(pg_bswap32(val4));
    }

    // 2. Поле TS (int8) - Смещение 4, Длина 8
    {
        uint64 val8;
        memcpy(&val8, raw_ptr + 4, 8);
        values[1] = Int64GetDatum(pg_bswap64(val8));
    }

    // 3. Поле Temperature (float4) - Смещение 12, Длина 4
    {
        union { uint32 i; float4 f; } u;
        memcpy(&u.i, raw_ptr + 12, 4);
        u.i = pg_bswap32(u.i);
        values[2] = Float4GetDatum(u.f);
    }

    // 4. Поле UUID - Смещение 16, Длина 16
    {
        void *uuid_ptr = palloc(16);
        memcpy(uuid_ptr, raw_ptr + 16, 16);
        values[3] = PointerGetDatum(uuid_ptr);
    }


    tuple = heap_form_tuple(layout->tupdesc, values, nulls);
    
    // Очистка временных массивов
    pfree(values);
    pfree(nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}