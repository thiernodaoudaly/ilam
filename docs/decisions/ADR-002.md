## ADR-002 — Trino over Spark SQL

```
Date    : 2025
Statut  : Accepté
Contexte: Choix du moteur de requête distribué
```

**Problème :** Il faut un moteur SQL capable d'interroger les tables Iceberg dans MinIO avec une latence interactive, tout en supportant la fédération de sources multiples.

**Alternatives évaluées :**

| Critère | Trino | Spark SQL | DuckDB | Presto (LF) |
| --- | --- | --- | --- | --- |
| Latence interactive | Secondes | Minutes (startup JVM) | Millisecondes | Secondes |
| Fédération sources | Excellente (SPI) | Limitée | Non | Bonne |
| Iceberg natif | Oui | Oui | Oui (lecture) | Oui |
| Scale horizontal | Oui | Oui | Non (mono-nœud) | Oui |
| Gouvernance | Linux Foundation | Apache SF | DuckDB Labs | Linux Foundation |
| Usage Sonatel-like | Analytique interactive | Batch ML lourd | Local/dev | Alternatif Trino |

**Décision :** Trino.

**Justification :** Latence interactive (secondes vs minutes), fédération native multi-sources via connecteurs SPI, intégration Iceberg de première classe. Conçu pour l'analytique ad hoc à grande échelle — exactement le besoin d'une DBM comme celle de Sonatel.

**Conséquences :** Trino est le moteur d'exécution de toutes les transformations dbt et de toutes les requêtes analytiques. Flink reste dédié au streaming.