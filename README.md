# pg_binmapper

**High-speed binary-to-record mapper for PostgreSQL 15+ and Citus.**

pg_binmapper is a PostgreSQL extension for ultra-low latency data ingestion. It allows you to map raw binary payloads (from Kafka, C++, Rust, Go, or C#) directly to table columns using high-performance memcpy operations, bypassing the overhead of JSON, Avro, or Protobuf parsing.

## Key Features
- **Direct Memory Mapping**: Maps raw bytes directly to table columns with zero-copy logic.
- **Table-Driven Schema**: Automatically builds a binary layout based on your existing table definition.
- **Citus Compatible**: Returns a standard RECORD, enabling Citus to handle sharding and distribution based on your shard key.
- **Layout Caching**: Caches binary layouts in memory for nanosecond access; automatically invalidates cache on ALTER TABLE.

---

## 1. Installation

### Build from source
apt-get update && apt-get install -y git build-essential postgresql-server-dev-16
git clone https://github.com
cd /pg_binmapper && make && make install

### Enable in Database
CREATE EXTENSION pg_binmapper;

---

## 2. Binary Protocol Specification

To ensure compatibility, the binary payload must follow these rules:
1. No Padding: Data must be tightly packed (equivalent to Pack = 1 or __attribute__((packed))).
2. Network Byte Order: Multi-byte integers and floats must be in Big-Endian.
3. Fixed Length: Only fixed-size types are supported (no variable-length strings).
4. Not Null: All fields in the target table must be NOT NULL.

---

## 3. High-Performance Setup (In-Memory Pipeline)

To achieve maximum speed, we use an INSTEAD OF INSERT trigger on a VIEW. This allows Kafka Connect to "insert" data into a virtual target, while pg_binmapper processes the bytes entirely in memory and redirects them to the final table.

### Step 1: Create a Virtual Ingest Pipe (VIEW)
This view acts as a high-speed endpoint for Kafka Connect.

CREATE VIEW target_table_ingest AS 
SELECT NULL::bytea AS payload;

### Step 2: Set up the In-Memory Trigger
This trigger intercepts the binary data before it touches any storage.

CREATE OR REPLACE FUNCTION trg_direct_binary_ingest() RETURNS trigger AS $$
BEGIN
    -- Binary payload is parsed in memory and inserted directly into the real table
    INSERT INTO target_table 
    SELECT * FROM bin_parse('target_table'::regclass, NEW.payload);
    
    RETURN NULL; -- Data is never stored in the VIEW
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_direct_ingest 
INSTEAD OF INSERT ON target_table_ingest
FOR EACH ROW EXECUTE FUNCTION trg_direct_binary_ingest();

### Step 3: Kafka Connect Configuration
Configure your JDBC Sink Connector to point to 'target_table_ingest'. Since the trigger handles the logic, Kafka Connect thinks it's doing a standard insert, but pg_binmapper is doing a zero-copy mapping behind the scenes.

---

## Contributing

Contributions are welcome! If you want to improve pg_binmapper:
1. Fork the repository.
2. Create a feature branch (e.g., `git checkout -b feature/support-new-type`).
3. Commit your changes.
4. Push to the branch and open a Pull Request.

Please ensure that any new code maintains the "Fixed-Layout / No-Lookup" philosophy for maximum performance.
