# ETL Framework - Ingestion Example

Questo esempio dimostra le capacità del framework ETL con un pipeline di ingestion completo.

## 📁 Struttura

```
example/
├── config/               # File di configurazione YAML
│   ├── global.yaml      # Configurazione globale
│   ├── domains.yaml     # Domini per validation
│   ├── customers.yaml   # Flow customers (master data)
│   ├── products.yaml    # Flow products (delta upsert)
│   └── orders.yaml      # Flow orders (foreign keys)
├── data/                # Dati CSV di input
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
└── output/              # Output generato
    ├── validated/       # Dati validati
    ├── rejected/        # Dati rigettati
    └── metadata/        # Metadati di esecuzione
```

## 🎯 Flows

### 1. Customers (Master Data)
- **Load Mode**: Full load
- **Validations**:
  - Primary key: `customer_id`
  - Email regex validation
  - Country domain validation (ISO codes)
  - Credit limit range (0 - 100,000)
- **Transformations**:
  - Pre-validation: email lowercase, name normalization
- **Output**: Partitioned by country
- **Test data**: 10 records, 2 invalidi (email, country)

### 2. Products (Delta Upsert)
- **Load Mode**: Delta upsert con timestamp
- **Validations**:
  - Primary key: `product_id`
  - Category domain validation
  - Price range (0.01 - 999,999.99)
  - Stock quantity >= 0
- **Transformations**:
  - Pre-validation: add timestamp, normalize data
- **Output**: Parquet con compressione snappy
- **Test data**: 10 records, 1 invalido (category)

### 3. Orders (Foreign Keys)
- **Load Mode**: Delta append
- **Validations**:
  - Primary key: `order_id`
  - Foreign key to customers
  - Foreign key to products
  - Status domain validation
  - Quantity range (1 - 1,000)
- **Transformations**:
  - Post-validation: join con customers e products per enrichment
- **Output**: Partitioned by status
- **Test data**: 10 records, 2 invalidi (FK violations)

## 🚀 Esecuzione

### Metodo 1: SBT
```bash
sbt "runMain com.etl.framework.example.IngestionExample"
```

### Metodo 2: Da IDE
Esegui la classe `com.etl.framework.example.IngestionExample`

## 📊 Risultati Attesi

### Success Metrics
- **Customers**: 8/10 validi (2 rigettati: email invalid, country XX)
- **Products**: 9/10 validi (1 rigettato: invalid_category)
- **Orders**: 8/10 validi (2 rigettati: customer_id 99, product_id 999)

### Rejection Rate
- Overall: ~20% (6 rejected / 30 total)
- All'interno del threshold configurato (15%)

### Output Files
```
example/output/
├── validated/
│   ├── customers/
│   │   ├── country=DE/
│   │   ├── country=ES/
│   │   ├── country=FR/
│   │   ├── country=IT/
│   │   └── country=US/
│   ├── products/
│   └── orders/
│       ├── status=CONFIRMED/
│       ├── status=DELIVERED/
│       ├── status=PENDING/
│       └── status=SHIPPED/
├── rejected/
│   ├── customers/
│   ├── products/
│   └── orders/
└── metadata/
    ├── customers_<batchId>.json
    ├── products_<batchId>.json
    └── orders_<batchId>.json
```

## 🔍 Cosa Viene Testato

### Validations
- ✅ Primary key uniqueness
- ✅ Foreign key integrity
- ✅ Regex patterns (email)
- ✅ Domain values (country, status, category)
- ✅ Range validation (price, quantity, credit_limit)
- ✅ Not null constraints

### Transformations
- ✅ Pre-validation SQL (normalization)
- ✅ Post-validation SQL (enrichment with joins)
- ✅ Column expressions (TRIM, UPPER, LOWER, INITCAP)

### Load Modes
- ✅ Full load (customers)
- ✅ Delta upsert with timestamp (products)
- ✅ Delta append (orders)

### Features
- ✅ Partitioning (by country, by status)
- ✅ Compression (snappy)
- ✅ Schema enforcement
- ✅ Rejection handling
- ✅ Metadata generation

## 🧪 Dati di Test Invalidi

I dati contengono deliberatamente alcuni record invalidi per testare le validazioni:

**customers.csv**:
- Record 6: email invalida (formato errato)
- Record 10: country "XX" (non nel dominio)

**products.csv**:
- Record 10: category "invalid_category" (non nel dominio)

**orders.csv**:
- Record 9: customer_id 99 (FK violation - cliente non esiste)
- Record 10: product_id 999 (FK violation - prodotto non esiste)

## 📝 Note

- L'esempio usa `local[*]` come master per Spark (esecuzione locale)
- I file di output vengono scritti nella directory `example/output/`
- Il batch ID viene generato automaticamente nel formato configurato
- I log mostrano informazioni dettagliate su validazioni e trasformazioni
