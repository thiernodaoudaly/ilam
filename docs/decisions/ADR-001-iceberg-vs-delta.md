## ADR-001 — Apache Iceberg over Delta Lake

```
Date    : 2025
Statut  : Accepté
Contexte: Choix du format de table ouvert pour le Lakehouse Ilam
```

**Problème :** Il faut un format de table qui garantisse ACID, schema evolution, time travel et query pruning sur MinIO, tout en restant interopérable avec Trino et Flink.

**Alternatives évaluées :**

| Critère | Apache Iceberg | Delta Lake | Apache Hudi |
| --- | --- | --- | --- |
| Gouvernance | ASF (neutre) | Linux Foundation (Databricks-led) | ASF |
| Spec ouverte | Oui | Delta Kernel (partiel) | Oui |
| Trino natif | Oui | Limité | Limité |
| Flink natif | Oui | Limité | Oui |
| Hidden partitioning | Oui | Non | Non |
| Adoption industrie 2024 | Snowflake, AWS, GCP, Azure | Databricks | Uber, Hudi community |

**Décision :** Apache Iceberg.

**Justification :** Spécification ouverte gouvernée par l'ASF, interopérabilité native avec tous les moteurs de la stack (Trino, Flink, dbt), zéro vendor lock-in. Standard de facto de l'industrie en 2024-2025.

**Conséquences :** Toutes les tables sont en format Iceberg sur MinIO. Le catalogue REST Iceberg sert de point d'entrée universel pour tous les moteurs.