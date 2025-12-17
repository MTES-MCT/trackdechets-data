## Description

<!-- Décrivez les changements introduits par cette PR en quelques phrases -->

### Type de changement
- [ ] Nouvelle fonctionnalité (ajout d'un pipeline, transformation, etc.)
- [ ] Correction de bug
- [ ] Refactorisation
- [ ] Amélioration de performance
- [ ] Documentation
- [ ] Infrastructure / Configuration
- [ ] Autre (précisez):

## Context et motivation

<!-- Expliquez pourquoi ce changement est nécessaire. Liez les issues/tickets pertinents -->

Closes #
Related to #

## Changements techniques

<!-- Détaillez les modifications apportées -->

### Code
- 

### Data
- Impact sur les sources de données:
- Impact sur les transformations:
- Impact sur les tables finales:

### Infrastructure
- 

## Testing

### Tests unitaires / Intégration
- [ ] Tests ajoutés/modifiés
- [ ] Tous les tests passent (`pytest`, `dbt test`, etc.)

```bash
# Commandes de test exécutées
```

### Tests en environnement de staging
- [ ] Validé en staging
- [ ] Comportement des données validé
- Observations:

### Performance
- Temps d'exécution avant: 
- Temps d'exécution après: 
- Impact sur les ressources (CPU, mémoire, coûts cloud):

## Données

### Réconciliation / Validation
- [ ] Reconciliation des volumes effectuée
- [ ] Intégrité des données validée
- [ ] Données de test utilisées (pas de données production)

### Backward compatibility
- [ ] Le changement est backward compatible
- [ ] Migrations de données nécessaires: 

### Documentation des données
- [ ] Schéma mis à jour
- [ ] Documentation des colonnes à jour
- [ ] Commentaires dbt/code pour la complexité

## Checklist avant merge

- [ ] Code reviewed (auto-review effectué)
- [ ] Tests passent localement et en CI/CD
- [ ] Pas de données sensibles committées
- [ ] Commits atomiques et messages clairs
- [ ] Branch à jour avec `main` (rebase si nécessaire)
- [ ] Linter/formatter exécuté (`ruff`, `sqlfluff`, etc.)
- [ ] Documentation et commentaires à jour
- [ ] Aucune dépendance problématique ajoutée

## Notes supplémentaires

<!-- Contexte, décisions techniques, limitations connues, etc. -->

### Dépendances
- Nécessite le déploiement de:
- Attendez que ceci soit mergé en premier:

### Points de discussion
-
