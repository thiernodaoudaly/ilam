## ADR-003 — MinIO over Amazon S3 / HDFS

```
Date    : 2025
Statut  : Accepté
Contexte: Choix de la solution de stockage objet
```

**Problème :** Il faut un stockage objet compatible S3, déployable on-premise, sans dépendance cloud, respectant la souveraineté des données — contrainte critique pour un opérateur télécom comme Sonatel.

**Alternatives évaluées :**

| Critère | MinIO | Amazon S3 | HDFS | Ceph |
| --- | --- | --- | --- | --- |
| Compatibilité S3 | Native | Native | Non | Oui |
| On-premise | Oui | Non | Oui | Oui |
| Souveraineté données | Totale | Nulle (AWS) | Totale | Totale |
| Erasure Coding | Oui (RS) | Oui | Réplication ×3 | Oui |
| Complexité opérationnelle | Faible | Nulle | Élevée | Très élevée |
| Open Source | AGPL-3.0 | Non | Apache 2.0 | LGPL |
| Performance | Très haute | Très haute | Bonne | Bonne |

**Décision :** MinIO.

**Justification :** Compatibilité S3 native (zéro changement de code pour les clients), déploiement simple (single binary), Erasure Coding plus efficace que la réplication HDFS (50% d'overhead vs 200%), souveraineté totale des données — critique pour la conformité réglementaire d'un opérateur télécom africain.

**Conséquences :** MinIO expose l'API S3 standard. Tous les composants (Trino, Flink, dbt, iceberg-rest) utilisent cette API sans modification. Remplacement transparent par S3 ou GCS si besoin.

---

## ADR-004 — tabulario/iceberg-rest over Project Nessie

```
Date    : 2025
Statut  : Accepté
Contexte: Choix du catalogue Iceberg (décision prise durant l'implémentation)
```

**Problème :** Trino 435 avec le connecteur Iceberg REST ne peut pas communiquer avec Nessie via l'endpoint `/api/v1` — incompatibilité de protocole.

**Alternatives évaluées :**

| Critère | tabulario/iceberg-rest | Project Nessie | Hive Metastore |
| --- | --- | --- | --- |
| Compatibilité Trino 435 | Native | Partielle (API v2 uniquement) | Via connecteur Hive |
| Simplicité | Très simple | Complexe | Complexe |
| Versioning Git-like | Non | Oui | Non |
| Production-ready | Dev/PFE | Oui | Oui |
| Dépendances | Aucune | PostgreSQL recommandé | PostgreSQL |

**Décision :** tabulario/iceberg-rest pour le PFE.

**Justification :** Compatibilité immédiate avec Trino 435, configuration minimale, zéro dépendance externe. Pour un environnement de production Sonatel, Nessie ou AWS Glue seraient préférables pour le versioning des tables.

**Conséquences :** Le catalogue est en mémoire (IN_MEMORY via SQLite). Les métadonnées sont perdues si le conteneur redémarre — acceptable en développement, à remplacer par une persistance PostgreSQL en production.