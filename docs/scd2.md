# SCD2 — Slowly Changing Dimension Type 2

## Cos'è SCD2

SCD2 è una strategia di caricamento che mantiene la **storia completa** di ogni record. Quando un attributo cambia, la versione precedente viene chiusa (riceve una data di fine validità) e ne viene inserita una nuova. Nessun dato viene mai cancellato o sovrascritto: tutte le versioni restano nella tabella e sono interrogabili.

Esempio: un cliente passa da tier SILVER a GOLD. Dopo il caricamento la tabella contiene:

| customer_id | tier | valid_from | valid_to | is_current |
|-------------|------|------------|----------|------------|
| 1 | SILVER | 2026-03-26 10:00 | 2026-03-27 14:00 | false |
| 1 | GOLD | 2026-03-27 14:00 | NULL | true |

La riga SILVER non è persa — è una versione storica consultabile tramite time travel o filtrando su `is_current = false`.

## Configurazione

### Configurazione minima

```yaml
loadMode:
  type: "scd2"
  validFromColumn: "valid_from"
  validToColumn: "valid_to"
  isCurrentColumn: "is_current"
  compareColumns:
    - "tier"
    - "credit_limit"
```

### Configurazione completa (con detect deletes)

```yaml
loadMode:
  type: "scd2"
  validFromColumn: "valid_from"
  validToColumn: "valid_to"
  isCurrentColumn: "is_current"
  isActiveColumn: "is_active"
  compareColumns:
    - "tier"
    - "credit_limit"
  detectDeletes: true
```

### Parametri

| Campo | Obbligatorio | Default | Descrizione |
|-------|-------------|---------|-------------|
| `validFromColumn` | no | `valid_from` | Colonna timestamp: quando questa versione è diventata valida |
| `validToColumn` | no | `valid_to` | Colonna timestamp: quando questa versione è stata superata. `NULL` = versione corrente |
| `isCurrentColumn` | no | `is_current` | Colonna boolean: `true` solo per la versione più recente di ogni record |
| `compareColumns` | sì | — | Lista di colonne da confrontare per rilevare modifiche. Solo queste colonne vengono controllate: se cambiano altri campi non elencati qui, il record non viene considerato modificato |
| `detectDeletes` | no | `false` | Se `true`, i record che spariscono dalla sorgente vengono marcati come eliminati (soft-delete) |
| `isActiveColumn` | no | `is_active` | Colonna boolean per il soft-delete. Usata solo quando `detectDeletes: true` |

### Vincolo sulle primary key

Le colonne della primary key **devono essere non-nullable**. Il framework valida questo vincolo al caricamento della configurazione e fallisce con un errore esplicito se non è rispettato. Il motivo è tecnico: SCD2 usa `NULL` come valore sentinella nelle merge key durante il MERGE INTO (vedi la sezione "Come funziona internamente").

### Colonne di sistema

Le colonne SCD2 (`valid_from`, `valid_to`, `is_current`, `is_active`) **non vanno dichiarate nello schema del flow** — il framework le aggiunge automaticamente alla tabella Iceberg. Nello schema si dichiarano solo le colonne business:

```yaml
schema:
  columns:
    - name: "customer_id"
      type: "integer"
      nullable: false
    - name: "tier"
      type: "string"
      nullable: false
    - name: "credit_limit"
      type: "double"
      nullable: false
```

## Comportamento per scenario

### Primo caricamento

Tutti i record vengono inseriti con:
- `valid_from = current_timestamp()`
- `valid_to = NULL`
- `is_current = true`
- `is_active = true` (se `detectDeletes: true`)

### Record modificato

Un record è considerato "modificato" quando almeno una delle `compareColumns` ha un valore diverso rispetto alla versione corrente nella tabella. Il confronto è null-safe: `NULL` è uguale a `NULL`, ma `NULL` è diverso da qualsiasi valore.

Quando viene rilevata una modifica:
1. La versione corrente viene **chiusa**: `valid_to = now`, `is_current = false`
2. Una nuova versione viene **inserita**: `valid_from = now`, `valid_to = NULL`, `is_current = true`

Entrambe le operazioni avvengono in un'unica transazione atomica.

### Record invariato

Se nessuna delle `compareColumns` è cambiata, il record non viene toccato. Nessuna nuova versione, nessun aggiornamento.

### Record nuovo

Un record con una primary key mai vista nella tabella viene inserito come prima versione: `valid_from = now`, `valid_to = NULL`, `is_current = true`.

### Record eliminato dalla sorgente (soft-delete)

**Solo se `detectDeletes: true`.** Quando un record presente nella tabella (con `is_current = true`) non compare nella sorgente, viene marcato come eliminato:
- `valid_to = now`
- `is_current = false`
- `is_active = false`

Il record non viene cancellato fisicamente — resta nella tabella come versione storica consultabile. La differenza rispetto a una versione chiusa per modifica è il campo `is_active = false`.

**Se `detectDeletes: false`** (default), i record assenti dalla sorgente vengono semplicemente ignorati. Restano nella tabella con `is_current = true` come se fossero ancora attivi.

### Record riattivato

Un record precedentemente soft-deleted (`is_active = false`) può riapparire nella sorgente. In questo caso viene inserita una nuova versione con `is_current = true` e `is_active = true`. La versione soft-deleted rimane nella storia.

Esempio dopo delete e riattivazione con cambiamento di tier:

| customer_id | tier | valid_to | is_current | is_active |
|-------------|------|----------|------------|-----------|
| 5 | GOLD | 2026-03-26 21:25 | false | false |
| 5 | PLATINUM | NULL | true | true |

### Idempotenza

Rieseguire lo stesso batch con gli stessi dati sorgente non produce nuove versioni. La change detection confronta ogni colonna in `compareColumns` e non trova differenze → nessuna azione. Questo vale anche per run consecutivi senza modifiche ai dati.

Con copy-on-write (default), Iceberg riscrive comunque i file fisici anche se nessuna riga cambia logicamente. Le statistiche snapshot mostreranno `added-records = N, deleted-records = N` — è un effetto del rewrite a livello di file, non un indicatore di cambiamenti reali. Per evitare il rewrite fisico su run idempotenti, configurare `write.merge.mode: merge-on-read` nelle `tableProperties`.

### Primary key composite

SCD2 supporta primary key su più colonne. Il framework crea una merge key per ogni colonna PK:

```yaml
validation:
  primaryKey:
    - "country_code"
    - "customer_id"
```

La staging view conterrà `_mk_country_code` e `_mk_customer_id`, entrambi NULL per i record nella parte 2 del UNION. Il MERGE matcha su entrambe le colonne:

```sql
ON target.country_code = source._mk_country_code
   AND target.customer_id = source._mk_customer_id
   AND target.is_current = true
```

Tutte le colonne della PK composita devono essere non-nullable (stesso vincolo della PK singola).

### Record duplicati nella sorgente

Se la sorgente contiene due righe con la stessa primary key e valori diversi, il comportamento dipende dall'ordine di processing di Spark e non è deterministico. Il framework non deduplica la sorgente prima del MERGE.

**Raccomandazione:** garantire l'unicità della primary key nella sorgente. Se non è possibile, aggiungere una regola di validazione custom o un pre-transform che selezioni la riga più recente.

### Sorgente vuota

Se la sorgente non contiene record:
- **Con `detectDeletes: false`**: nessuna modifica alla tabella.
- **Con `detectDeletes: true`**: **tutti i record correnti vengono soft-deleted** (`is_active = false`). Questo è il comportamento corretto dal punto di vista semantico (la sorgente dichiara che nessun record esiste più), ma potrebbe essere distruttivo se la sorgente è vuota per errore (es. file non caricato).

**Mitigazione:** il framework valida il `maxRejectionRate` configurato in `global.yaml`. Se tutti i record vengono rifiutati (tasso di rigetto 100%), il batch si blocca prima del MERGE. Tuttavia una sorgente vuota (0 record) non ha record da rifiettare e passa la validazione. Un controllo esplicito sulla cardinalità minima della sorgente va implementato a livello di pipeline se necessario.

### Valori NULL nelle compareColumns

Il confronto usa operatori null-safe:
- `NULL` vs `NULL` → **uguale** (nessuna modifica)
- `NULL` vs `'valore'` → **diverso** (nuova versione)
- `'valore'` vs `NULL` → **diverso** (nuova versione)

Questo significa che passare da `credit_limit = 5000` a `credit_limit = NULL` genera una nuova versione (corretto), e rieseguire con `credit_limit = NULL` non genera un'altra versione (idempotente).

### Modificare compareColumns su una tabella esistente

Aggiungere o rimuovere colonne dalla lista `compareColumns` ha effetto dal run successivo:

- **Aggiungere una colonna** (es. aggiungere `email` alla lista): al prossimo run, tutti i record con `email` diversa tra sorgente e tabella corrente verranno considerati "modificati" e genereranno nuove versioni. Se molti record hanno differenze sulla nuova colonna, questo può causare un burst di versioni.
- **Rimuovere una colonna** (es. rimuovere `email`): le differenze su `email` non generano più nuove versioni. Le versioni storiche create per cambiamenti di `email` restano nella tabella.

Non è richiesta nessuna migrazione — il framework usa la lista corrente di `compareColumns` a ogni run.

## compareColumns — cosa monitorare

`compareColumns` definisce **quali colonne determinano se un record è "cambiato"**. Solo queste colonne vengono confrontate. I campi non elencati possono cambiare senza generare una nuova versione.

Esempio: se `compareColumns: [tier, credit_limit]` e cambia solo l'email, non viene creata nessuna nuova versione. Se cambia il `tier`, viene creata una nuova versione.

**Cosa includere:**
- Attributi business il cui cambiamento ha valore storico (tier, status, prezzo, indirizzo)

**Cosa escludere:**
- Colonne che cambiano a ogni batch senza valore storico (timestamp di aggiornamento, flag tecnici)
- Colonne della primary key (non possono cambiare per definizione)

**Se `compareColumns` è vuoto:** tutte le colonne non-PK e non-SCD2 vengono confrontate. Questo è il default ma tende a generare versioni anche per cambiamenti irrilevanti.

## is_current vs is_active

Queste due colonne boolean codificano informazioni diverse:

| is_current | is_active | Significato |
|------------|-----------|-------------|
| true | true | Versione corrente, il record esiste nella sorgente |
| false | true | Versione storica, superata da una più recente |
| false | false | Record eliminato dalla sorgente (soft-delete) |
| true | false | Non possibile in condizioni normali |

**`is_current`** traccia il versionamento. Ogni primary key ha esattamente una riga con `is_current = true` (invariante garantita dal framework).

**`is_active`** traccia l'eliminazione logica. Permette query come "mostrami l'ultimo stato noto di tutti i clienti eliminati" (`is_current = true` nella loro ultima versione, `is_active = false`).

`is_active` è presente solo quando `detectDeletes: true` o quando `isActiveColumn` è esplicitamente configurato.

## Abilitare detectDeletes su una tabella esistente

Se abiliti `detectDeletes` su una tabella SCD2 che contiene già dati, la colonna `is_active` viene aggiunta automaticamente via schema evolution (`ALTER TABLE ADD COLUMN`). Tuttavia le righe già presenti avranno `is_active = NULL`, non `true`.

Questo significa che query con `WHERE is_active = true` escluderanno i record esistenti — un **problema di correttezza dei dati**.

Il framework emette un warning quando rileva questa situazione:

```
WARN  Column 'is_active' (is_active) was added to an existing table (catalog.default.customers).
      Rows written before this change have is_active=NULL and will be excluded by queries
      filtering on is_active = true.
      Run a full reload to backfill is_active=true on all current records.
```

**Raccomandazione:** dopo aver abilitato `detectDeletes` su una tabella esistente, esegui un full reload per backfillare `is_active = true` su tutte le righe correnti:

1. Cambia temporaneamente `loadMode.type` a `full`
2. Esegui il pipeline con il dataset sorgente completo
3. Ripristina `loadMode.type` a `scd2`

## Esempio completo: flow YAML

```yaml
name: "customers_scd2"
description: "Customer tier history — SCD2 tracking tier and credit_limit changes"
version: "1.0"
owner: "data-team"

source:
  type: "file"
  path: "data/customers_scd2.csv"
  format: "csv"
  options:
    header: "true"
    delimiter: ","

schema:
  enforceSchema: true
  allowExtraColumns: false
  columns:
    - name: "customer_id"
      type: "integer"
      nullable: false
      description: "Customer identifier"
    - name: "email"
      type: "string"
      nullable: false
      description: "Customer email"
    - name: "tier"
      type: "string"
      nullable: false
      description: "Customer tier (BRONZE/SILVER/GOLD/PLATINUM)"
    - name: "credit_limit"
      type: "double"
      nullable: false
      description: "Credit limit (EUR)"

loadMode:
  type: "scd2"
  validFromColumn: "valid_from"
  validToColumn: "valid_to"
  isCurrentColumn: "is_current"
  isActiveColumn: "is_active"
  compareColumns:
    - "tier"
    - "credit_limit"
  detectDeletes: true

validation:
  primaryKey:
    - "customer_id"
  foreignKeys: []
  rules: []

output:
  icebergPartitions:
    - "year(valid_from)"
```

## Query utili

### Stato attuale di tutti i clienti

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE is_current = true
```

### Storia completa di un singolo cliente

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE customer_id = 1
ORDER BY valid_from
```

### Clienti eliminati (soft-delete)

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE is_current = false AND is_active = false
```

### Stato dei clienti a una data specifica

```sql
SELECT * FROM catalog.default.customers_scd2
WHERE valid_from <= TIMESTAMP '2026-03-26 12:00:00'
  AND (valid_to IS NULL OR valid_to > TIMESTAMP '2026-03-26 12:00:00')
```

### Clienti che hanno cambiato tier

```sql
SELECT customer_id, COUNT(*) AS versions
FROM catalog.default.customers_scd2
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY versions DESC
```

### Confronto tra batch (tramite snapshot tag)

```sql
SELECT curr.customer_id, curr.tier AS tier_now, prev.tier AS tier_before
FROM catalog.default.customers_scd2 curr
JOIN catalog.default.customers_scd2 VERSION AS OF 'batch_20260326_100000' prev
  ON curr.customer_id = prev.customer_id
  AND curr.is_current = true AND prev.is_current = true
WHERE curr.tier != prev.tier
```

## Partizionamento e performance

### Strategia di partizionamento consigliata

La scelta più comune per SCD2 è partizionare su `valid_from`:

```yaml
output:
  icebergPartitions:
    - "year(valid_from)"
```

Questo fa sì che:
- Le nuove versioni finiscano nella partizione dell'anno corrente
- Le versioni storiche restino nella partizione dell'anno in cui sono state create
- Le query point-in-time (`WHERE valid_from <= X AND valid_to > X`) beneficino del partition pruning

Non partizionare su `is_current` — ha solo 2 valori e genera partizioni sbilanciate. Non partizionare su colonne ad alta cardinalità (customer_id, email).

### Performance del MERGE

Il MERGE INTO per SCD2 lavora solo sulle righe con `is_current = true` grazie alla condizione `AND target.is_current = true` nella clausola `ON`. Le versioni storiche (potenzialmente milioni) non vengono toccate.

Questo significa che la performance del MERGE scala con il numero di record **correnti**, non con il numero totale di versioni nella tabella. Una tabella con 1 milione di record correnti e 50 milioni di versioni storiche ha la stessa performance di MERGE di una tabella con solo 1 milione di record.

### Invariante di consistenza

Il framework garantisce che per ogni valore di primary key esiste **esattamente una riga** con `is_current = true`. Questa invariante è mantenuta dall'atomicità del MERGE INTO: la chiusura della vecchia versione e l'inserimento della nuova avvengono nella stessa transazione.

Se il batch fallisce a metà, Iceberg esegue il rollback automatico e la tabella resta nello stato precedente — nessuna riga con `is_current` in stato inconsistente.

## SCD2 history vs Iceberg time travel

Entrambi i meccanismi permettono di vedere dati storici, ma rispondono a domande diverse:

| | SCD2 history | Iceberg time travel |
|---|---|---|
| **Cosa traccia** | Cambiamenti di business (tier, status, prezzo) | Cambiamenti tecnici (batch, operazioni di scrittura) |
| **Granularità** | Per-record: ogni modifica ha valid_from/valid_to | Per-snapshot: stato dell'intera tabella a un dato batch |
| **Retention** | Permanente — le versioni restano nella tabella | Configurabile — le snapshot scadono dopo N giorni |
| **Query** | `WHERE customer_id = 1 ORDER BY valid_from` | `VERSION AS OF 'batch_20260326'` |
| **Uso tipico** | Audit, analisi trend, reporting dimensionale | Debug, rollback, confronto batch |

Le due funzionalità sono complementari. SCD2 vive nei dati, time travel vive nei metadati Iceberg.

## Come funziona internamente

### Il problema dell'atomicità

Una implementazione naive di SCD2 richiederebbe due operazioni separate:
1. Un MERGE per chiudere le versioni vecchie dei record modificati
2. Un INSERT per inserire le nuove versioni

Se il sistema fallisce tra il punto 1 e il punto 2, la tabella resta in uno stato inconsistente: le vecchie versioni sono chiuse ma le nuove non sono ancora inserite.

### La soluzione: NULL merge-key trick

Il framework risolve il problema con un singolo `MERGE INTO` atomico, usando una staging view che contiene ogni record sorgente modificato **due volte**:

1. **Con merge key = primary key** — per matchare la versione corrente nella tabella e chiuderla
2. **Con merge key = NULL** — per forzare la clausola `NOT MATCHED` e inserire la nuova versione

La staging view è costruita come:

```sql
-- Parte 1: tutti i record sorgente (merge key = PK reale)
SELECT src.*, src.customer_id AS _mk_customer_id
FROM source src

UNION ALL

-- Parte 2: solo i record MODIFICATI (merge key = NULL)
SELECT src.*, CAST(NULL AS INT) AS _mk_customer_id
FROM source src
JOIN target tgt
  ON src.customer_id = tgt.customer_id AND tgt.is_current = true
WHERE src.tier != tgt.tier
   OR (src.tier IS NULL AND tgt.tier IS NOT NULL)
   OR (src.tier IS NOT NULL AND tgt.tier IS NULL)
   OR src.credit_limit != tgt.credit_limit
   OR (src.credit_limit IS NULL AND tgt.credit_limit IS NOT NULL)
   OR (src.credit_limit IS NOT NULL AND tgt.credit_limit IS NULL)
```

Il MERGE finale opera su questa staging view con tre clausole:

```sql
MERGE INTO catalog.default.customers_scd2 AS target
USING staged_source AS source
ON target.customer_id = source._mk_customer_id
   AND target.is_current = true

-- Clausola 1: chiude la versione vecchia del record modificato
WHEN MATCHED AND (change condition) THEN UPDATE SET
  target.valid_to = current_timestamp(),
  target.is_current = false

-- Clausola 2: inserisce la nuova versione (merge key NULL → NOT MATCHED)
--             oppure un record completamente nuovo
WHEN NOT MATCHED THEN INSERT
  (customer_id, email, tier, credit_limit, valid_from, valid_to, is_current, is_active)
  VALUES (source.customer_id, source.email, source.tier, source.credit_limit,
          current_timestamp(), CAST(NULL AS TIMESTAMP), true, true)

-- Clausola 3 (opzionale, solo con detectDeletes=true):
-- soft-delete dei record non presenti nella sorgente
WHEN NOT MATCHED BY SOURCE AND target.is_current = true THEN UPDATE SET
  target.valid_to = current_timestamp(),
  target.is_current = false,
  target.is_active = false
```

**Perché funziona:**
- Un record modificato appare due volte nella staging view: una volta con la PK reale (matcha la clausola 1, chiude la versione vecchia), e una volta con merge key NULL (matcha la clausola 2, inserisce la nuova versione)
- Un record nuovo appare una volta con la PK reale, ma non ha match nella tabella → clausola 2, insert
- Un record invariato appare una volta con la PK reale, matcha la clausola 1, ma la change condition è `false` → nessuna azione
- Un record assente dalla sorgente è presente solo nella tabella → clausola 3 (se abilitata), soft-delete

Tutto avviene in un singolo statement SQL atomico. In caso di errore, Iceberg esegue il rollback automatico e la tabella resta nello stato precedente.

## Continuità temporale

Per ogni coppia di versioni consecutive dello stesso record, il `valid_to` della versione vecchia è uguale al `valid_from` della nuova:

```
versione 1: valid_from=T1, valid_to=T2
versione 2: valid_from=T2, valid_to=NULL
```

Questo è garantito dal fatto che entrambe le operazioni (chiudi vecchia, apri nuova) usano `current_timestamp()` all'interno dello stesso statement MERGE, che viene valutato una sola volta. Non ci sono gap temporali tra le versioni.

## Limitazioni note

### Singolo writer

Il framework assume un singolo writer per tabella. Esecuzioni concorrenti dello stesso flow possono violare l'invariante `is_current` (due versioni marcate come correnti per la stessa PK). In ambienti con scheduling concorrente, garantire che ogni flow SCD2 abbia al massimo un'istanza attiva.

### Late-arriving data

SCD2 nel framework usa `current_timestamp()` come `valid_from` per le nuove versioni. Non supporta l'inserimento di versioni storiche con `valid_from` nel passato. Se un record arriva in ritardo con uno stato che era valido ieri, viene inserito con `valid_from = now`, non `valid_from = ieri`.

### Nessun hard-delete

SCD2 non cancella mai fisicamente righe dalla tabella. Con `detectDeletes: true` i record vengono solo marcati (`is_active = false`). La tabella cresce monotonamente. Per ambienti con requisiti di data retention (es. GDPR), la cancellazione fisica va gestita separatamente tramite operazioni manuali o procedure Iceberg come `DELETE FROM ... WHERE`.

### Cambiamento di primary key

Se la primary key di un record cambia nella sorgente (es. un customer_id viene riassegnato), il framework lo tratta come due eventi separati: un soft-delete del vecchio PK e un insert del nuovo PK. Non c'è correlazione tra i due — la storia del vecchio PK viene chiusa e il nuovo PK inizia una storia indipendente.
