# Gap Analysis — cosa manca per essere production-ready

Stato attuale: il framework funziona end-to-end in locale (esempio eseguibile con `./run.sh`), ha un'architettura solida, e l'integrazione Iceberg è completa. Quello che segue è un'analisi onesta di cosa manca o è rotto prima di poterlo consegnare a un utente reale.

---

## Blocchi critici

Questi problemi impediscono l'uso in produzione o causano crash silenti.

### 1. Dipendenze AWS per GlueCatalogProvider non documentate

`GlueCatalogProvider` dichiara l'uso di `org.apache.iceberg.aws.glue.GlueCatalog` e `org.apache.iceberg.aws.s3.S3FileIO`. Queste classi non appartengono alla libreria: sono responsabilità del progetto che la usa, esattamente come Spark è `provided`.

Su AWS Glue e EMR il runtime le porta già. In un progetto standalone che usa il framework come dipendenza, l'utente deve aggiungere al proprio `build.sbt`:

```scala
"org.apache.iceberg" % "iceberg-aws-bundle" % "1.5.0"
```

Questo non è documentato da nessuna parte. Un utente che prova a usare `catalog-type: glue` senza saperlo ottiene un `ClassNotFoundException` a runtime.

Manca: una sezione nella guida deploy che elenchi le dipendenze necessarie per ogni target (Glue, EMR, standalone).

### 2. Quattro aggregation functions non implementate in `JoinStrategyExecutor`

`First`, `Last`, `CollectList`, `CollectSet` sono dichiarate nell'enum `AggregationFunction` ma nel pattern match lanciano `UnsupportedOperationException`. Un utente che le usa in un DAG YAML ottiene un crash a runtime senza alcun indizio in fase di configurazione.

File: [JoinStrategyExecutor.scala](../src/main/scala/com/etl/framework/aggregation/JoinStrategyExecutor.scala)

### 3. `sbt assembly` produce un JAR inutilizzabile

`build.sbt` non configura `assembly`: manca la `mainClass` e la merge strategy per i file di servizio (META-INF, reference.conf, ecc.). Il comando funziona ma produce un JAR che crasha all'avvio per conflitti di classpath o per assenza di entry point.

```scala
// da aggiungere in build.sbt
assembly / mainClass := Some("com.etl.framework.example.IngestionExample")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case "reference.conf"                     => MergeStrategy.concat
  case _                                    => MergeStrategy.first
}
```

---

## Manca per il deploy su piattaforme reali

Il framework non ha nessuna guida né template per deployarlo fuori dalla JVM locale.

### AWS Glue

Un utente che usa Glue deve sapere:

- Come configurare il Glue Job (runtime Spark 3.x, worker type G.1X/G.2X)
- Quali JAR caricare su S3 come `--extra-jars` (il framework non porta Spark, ma porta Iceberg e le sue dipendenze)
- Come passare la config YAML (S3 path via `--conf spark.hadoop.fs.s3a.path=...` o argomento main)
- Come strutturare il `main()` Scala (il Glue runtime chiama `main`, non `run`)
- Come configurare IAM: il Glue role ha bisogno di `glue:GetTable`, `glue:CreateTable`, `s3:GetObject`, `s3:PutObject`, ecc.
- Come passare le variabili d'ambiente per i secret (`${DB_PASSWORD}`, ecc.)

Manca: guida, template IAM policy, esempio di Glue Job definition.

### EMR

Su EMR il `spark-submit` è esplicito. Serve sapere:

- Come costruire il fat JAR con le dipendenze corrette (Spark `provided`, Iceberg incluso o come `--packages`)
- Come passare `--conf spark.sql.extensions=...` prima della creazione della sessione
- Come caricare i YAML di configurazione (S3 o bootstrap action)
- Quale versione EMR è compatibile con Spark 3.5.x

Manca: guida deploy, script `spark-submit` di esempio.

### Databricks

Su Databricks la SparkSession è pre-creata dal runtime. Serve sapere:

- Come installare il JAR come cluster library
- Come impostare `spark.sql.extensions` a livello di cluster (non nel codice)
- Come gestire i YAML (DBFS, Volumes, o configurazione inline)
- Come `requiredSparkConfig` si integra con la cluster configuration UI

Manca: guida, note su Unity Catalog vs Iceberg.

---

## Esperienza utente del framework

### Nessun README alla radice del progetto

Non esiste `README.md` nella root. Un utente che clona il repo non sa da dove partire. `CLAUDE.md` è documentazione interna per lo sviluppo, non un documento rivolto all'utente finale.

Il README dovrebbe contenere: cosa fa il framework, come si aggiunge come dipendenza, quick start (3-4 passi), link alla documentazione.

### Nessun esempio con Iceberg abilitato

`example/` dimostra il framework con output Parquet. Non esiste un esempio funzionante con Iceberg, quindi un utente che vuole usarlo deve costruirsi la config da zero basandosi solo sulla documentazione in `docs/iceberg-integration.md`.

Serve almeno un `example/config/global-iceberg.yaml` con catalog hadoop locale e un flow che usa MERGE INTO.

### La configurazione multi-ambiente non è guidata

Il framework supporta la sostituzione di variabili d'ambiente nei YAML (`${VAR_NAME}`), ma questo meccanismo non è documentato in modo prominente e non ci sono template per dev/staging/prod. Un utente non sa come strutturare la propria configurazione per ambienti diversi.

### JDBC driver non inclusi

`JDBCDataReader` funziona tramite le opzioni Spark, ma nessun driver JDBC è nel classpath. Un utente che punta a Postgres ottiene un `ClassNotFoundException`. Serve almeno documentare quali dipendenze aggiungere o includere i driver più comuni come opzionali (`provided` o in un modulo separato).

### Nessun meccanismo di validazione YAML a startup

Se un campo YAML è sbagliato (typo nel nome di un enum, tipo colonna non riconosciuto, path non esistente), l'errore emerge a runtime durante l'esecuzione del flow, non al bootstrap. Un utente passa ore a debuggare prima di trovare che `loadMode: fulll` con una `l` di troppo non viene riconosciuto.

La config viene già caricata via PureConfig che fa parsing, ma la validazione semantica (es. che i path esistano, che i tipi colonna siano validi) non è sistematica.

---

## Funzionalità incomplete


### `OrphanDetector` non gestisce assenza di snapshot precedente

Se un flow viene eseguito per la prima volta (nessun snapshot precedente), `OrphanDetector` tenta comunque il time travel e fallisce silenziosamente. Il log non è esplicito su questo caso.

---

## Monitoring e observability

Non esiste nessun modo per un sistema esterno di monitorare lo stato del framework durante l'esecuzione. I metadata JSON scritti da `BatchMetadataWriter` sono utili come audit trail post-run, ma non come monitoring real-time.

In produzione serve almeno:

- **Rejection rate alerting**: se il `maxRejectionRate` viene superato in modo inaspettato (es. schema change upstream), qualcuno deve saperlo immediatamente, non leggendo i log dopo.
- **Execution time anomalies**: un batch che impiega il triplo del normale è un segnale di problema (skew, source degradata, ecc.).
- **Integrazione con il sistema di alerting esistente**: CloudWatch, Datadog, Prometheus — dipende dall'infrastruttura dell'utente.

Il framework attualmente non ha nessun hook di notifica (webhook, SNS topic, ecc.) né esporta metriche in formato strutturato.

---

## CI/CD

Non esiste nessuna pipeline di CI. Manca:

- **GitHub Actions** (o equivalente) che esegua `sbt test` e `sbt scalafmtCheckAll` su ogni PR
- **Artifact publishing**: il JAR non viene pubblicato da nessuna parte; ogni utente deve buildarlo da sorgente
- **Versioning**: non esiste un meccanismo di versionamento (SemVer, tag Git, ecc.)

---

## Priorità suggerite

| # | Cosa | Impatto | Effort |
|---|------|---------|--------|
| 1 | Documentare dipendenze AWS necessarie per Glue (lato utente) | Blocca uso su Glue senza debug | Basso |
| 2 | Implementare le 4 aggregation functions mancanti | Crash a runtime | Basso |
| 3 | Configurare `sbt assembly` con mainClass e mergeStrategy | Blocca deploy | Basso |
| 4 | Aggiungere guida deploy AWS Glue | Necessaria per utenti Glue | Medio |
| 5 | Esempio funzionante con Iceberg (hadoop locale) | UX fondamentale | Medio |
| 6 | README.md alla radice | Prima cosa che vede un utente | Basso |
| 7 | Fail-fast su variabili d'ambiente mancanti in ConfigLoader | Debug difficile | Basso — già fixato (commit 80d798b) |
| 8 | Guida configurazione multi-ambiente (dev/staging/prod) | UX | Basso |
| 9 | Documentare dipendenze JDBC necessarie | Confusion point | Basso |
| 10 | GitHub Actions CI | Qualità del progetto | Medio |
