{#
    Macros de probabilité implicite (marge bookmaker retirée / "no-vig").

    Une cote se convertit en probabilité par 1/cote. Mais la somme des inverses
    d'un marché dépasse 100 % : l'excédent est la marge du book. On la retire en
    normalisant chaque inverse par la somme des inverses du marché.

    Réplique en SQL la logique de implied_proba() du pipeline Python (01b_odds.py).
    NULLIF(...,0) protège des divisions par zéro et des cotes manquantes (→ NULL).
#}

{# ── 3-way : marché 1N2 (home / draw / away) ─────────────────────────────── #}
{% macro implied_prob_3way(home, draw, away, target) %}
    {%- set inv_h = "(1.0 / NULLIF(" ~ home ~ ", 0))" -%}
    {%- set inv_d = "(1.0 / NULLIF(" ~ draw ~ ", 0))" -%}
    {%- set inv_a = "(1.0 / NULLIF(" ~ away ~ ", 0))" -%}
    {%- set total = "(" ~ inv_h ~ " + " ~ inv_d ~ " + " ~ inv_a ~ ")" -%}
    {%- if   target == 'home' -%} {{ inv_h }} / NULLIF({{ total }}, 0)
    {%- elif target == 'draw' -%} {{ inv_d }} / NULLIF({{ total }}, 0)
    {%- elif target == 'away' -%} {{ inv_a }} / NULLIF({{ total }}, 0)
    {%- else -%} {{ exceptions.raise_compiler_error("implied_prob_3way: target invalide '" ~ target ~ "'") }}
    {%- endif -%}
{% endmacro %}

{# ── 2-way : marché binaire (ex. Over/Under 2.5). Retourne la proba de `a`. ── #}
{% macro implied_prob_2way(a, b) %}
    {%- set inv_a = "(1.0 / NULLIF(" ~ a ~ ", 0))" -%}
    {%- set inv_b = "(1.0 / NULLIF(" ~ b ~ ", 0))" -%}
    {{ inv_a }} / NULLIF(({{ inv_a }} + {{ inv_b }}), 0)
{% endmacro %}
