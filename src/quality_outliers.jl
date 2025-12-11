# validate_ranges, cap_outliers_salary

"""
    validate_range(data::SalaryTbl) -> DataFrame
    validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector{<:Function}) -> DataFrame
    validate_range(var::AbstractVector, test::Function) -> Vector

Vérifie la plausibilité des valeurs dans des données (intervalles, ensembles finis, positivité).

Cette fonction possède trois comportements selon le type d'entrée :

1. **Sur un `SalaryTbl`** :
   Applique un ensemble de règles métier prédéfinies (hardcoded) sur les colonnes spécifiques
   du dataset salaire (par exemple : vérifier que `employment_type` est dans `EMPLOYMENT_TYPES`,
   que `salary` est positif, etc.).

2. **Sur un `DataFrame` générique** :
   Applique une liste de fonctions de test (`tests_a_effectuer`) sur une liste correspondante
   de colonnes (`vars_a_tester`).

3. **Sur un `Vector` unique** :
   Applique une fonction de test à un vecteur et renvoie le vecteur et le résultat global.

# Arguments

- `data` (`SalaryTbl` ou `DataFrame`) : Les données à valider.
- `vars_a_tester` : Vecteur de noms de colonnes (Symbol ou String) à tester (pour la version générique).
- `tests_a_effectuer` : Vecteur de fonctions anonymes renvoyant un `Bool`, appliquées élément par élément (ex: `x -> x > 0`).
- `var` : Vecteur unique à tester.
- `test` : Fonction de test unique.

# Retour

- **Version `SalaryTbl` et `DataFrame`** : Renvoie un `DataFrame` récapitulatif contenant deux colonnes :
    - `variables` : le nom de la variable testée.
    - `valid_mask` : `true` si **toutes** les valeurs non manquantes de la colonne respectent la condition, `false` sinon.
- **Version `Vector`** : Renvoie un vecteur à deux éléments `[vecteur_origine, booléen_global]`.

# Notes

- Les valeurs `missing` sont ignorées lors des tests (via `skipmissing`).
- Pour `SalaryTbl`, les constantes globales `EMPLOYMENT_TYPES`, `EXPERIENCE` et `SIZE` doivent être définies dans le module.

# Exemples

Validation métier (`SalaryTbl`) :

```julia
tbl = SalaryTbl(df_valide)
res = validate_range(tbl)
# DataFrame avec colonnes :variables et :valid_mask
Validation générique (DataFrame) :

Julia
￼
df = DataFrame(age = [25, -5], prix = [10, 20])
vars = [:age, :prix]
tests = [x -> x > 0, x -> x >= 0]

validate_range(df, vars, tests)
# Renvoie un DataFrame indiquant que :age est false (à cause de -5) et :prix est true.
```
"""
function validate_range end


function validate_range(data::SalaryTbl)
    valid_mask = Bool[]
    push!(valid_mask, all(x -> x in EMPLOYMENT_TYPES, skipmissing(data.df[!, :employment_type])))
    push!(valid_mask, all(x -> x in EXPERIENCE, skipmissing(data.df[!, :experience_level])))
    push!(valid_mask, all(x -> x >0, skipmissing(data.df[!, :salary])))
    push!(valid_mask, all(x -> x >0, skipmissing(data.df[!, :salary_in_usd])))
    push!(valid_mask, all(x -> 0 <= x && x <= 100, skipmissing(data.df[!, :remote_ratio])))
    push!(valid_mask, all(x -> x in SIZE, skipmissing(data.df[!, :company_size])))
    df = DataFrame(variables = ["employment_type", "experience_level", "salary", "salary_in_usd", "remote_ratio", "company_size"], valid_mask = valid_mask)
    return df
end



function validate_range(data::DataFrame, vars_a_tester::AbstractVector, tests_a_effectuer::AbstractVector{<:Function})
    valid_mask = Bool[]
    for index in 1:length(vars_a_tester)
        push!(valid_mask, all(tests_a_effectuer[index], skipmissing(data[!, vars_a_tester[index]])))
    end
    df = DataFrame(variables = vars_a_tester, valid_mask = valid_mask)
    return df
end


function validate_range(var::AbstractVector, test::Function)
    verif = [var, all(test, skipmissing(var))]
    return verif
end


"""
    winsorize(vect::AbstractVector; lower_quantile=0.05, upper_quantile=0.95) -> Vector
    winsorize(data::AbstractDataFrame; lower_quantile=0.05, upper_quantile=0.95) -> DataFrame

Traite les valeurs aberrantes (outliers) en utilisant la méthode de la winsorisation.

Les valeurs situées en dessous du quantile inférieur (`lower_quantile`) sont remplacées par la valeur de ce quantile.
Les valeurs situées au-dessus du quantile supérieur (`upper_quantile`) sont remplacées par la valeur de ce quantile.

# Arguments

- `vect` : Vecteur numérique à traiter.
- `data` : `DataFrame` dont on veut traiter les colonnes numériques.
- `lower_quantile` (défaut `0.05`) : Seuil bas (5%).
- `upper_quantile` (défaut `0.95`) : Seuil haut (95%).

# Retour

- **Version `Vector`** : Renvoie un nouveau vecteur winsorisé.
- **Version `DataFrame`** : Modifie les colonnes numériques du `DataFrame` **en place** et le retourne.

# Détails

- Sur un `DataFrame`, la fonction itère sur toutes les colonnes.
- Elle n'applique la transformation que si la colonne est de type `Real` (numérique) et contient plus d'une valeur.
- Les valeurs `missing` sont ignorées pour le calcul des quantiles (`skipmissing`), mais conservées à leur position dans le résultat (le remplacement respecte la structure).

# Exemples

Sur un vecteur :

```julia
v = [1, 2, 3, 1000] # 1000 est une valeur extrême
w = winsorize(v; upper_quantile=0.75)
# Les valeurs extrêmes sont ramenées au seuil calculé.
Sur un DataFrame :

Julia

df = DataFrame(a = [1, 2, 100], b = ["x", "y", "z"])
winsorize(df)
# La colonne :a sera modifiée, la colonne :b (texte) restera inchangée.
```

"""
function winsorize end

function winsorize(vect::AbstractVector; lower_quantile=0.05, upper_quantile=0.95)
    lower = quantile(vect, lower_quantile)
    upper = quantile(vect, upper_quantile)
    return max.(min.(vect, upper), lower)
end

function winsorize(data::AbstractDataFrame; lower_quantile=0.05, upper_quantile=0.95)
    for col_name in names(data)
        col = data[!, col_name]
        if eltype(col) <: Real && length(col) > 1
            lower = quantile(col, lower_quantile)
            upper = quantile(col, upper_quantile)
            data[!, col_name] = max.(min.(col, upper), lower)
        end
    end
    return(data)
end