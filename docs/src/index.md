```@meta
CurrentModule = PackageDataCleaning
```

# PackageDataCleaning

PackageDataCleaning.jl est une librairie Julia pour faciliter le nettoyage de données tabulaires : gestion des valeurs manquantes, validation de schémas, nettoyage de colonnes texte, etc.

## Installation

```julia
pkg> add PackageDataCleaning
```

### Prise en main rapide

```julia
using PackageDataCleaning
using DataFrames

df = DataFrame(
    age    = [25, 30, missing],
    income = [3000, missing, 4500],
)

# Exemple : fonction de nettoyage 
df_clean = impute_missing!(df)

# Exemple : fonction de validation de schéma
df = DataFrame(a = [1], b = [2])

validate_schema(df, [:a, :b]; strict=true)
# > true
```
### Où trouver quoi ?

**Introduction** : vue d’ensemble et exemples simples.
- **API** : description détaillée de chaque fonction (types, arguments, etc.).

Documentation for [PackageDataCleaning](https://github.com/loic-mmt/PackageDataCleaning.jl).
