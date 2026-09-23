{% test no_suspicious_similar_names(model, key_column, name_column) %}
-- Heuristique de détection : deux key_column DIFFÉRENTS dont le name_column,
-- après normalisation par tokens, est identique.
--
-- Normalisation appliquée :
--   1) Retrait des accents (translate ASCII)
--   2) Passage en minuscules
--   3) Retrait de la ponctuation ('.', '-', apostrophes, backticks, underscore, slash)
--   4) Retrait des tokens fréquents non discriminants
--      (fc, cf, ac, sc, as, ca, rc, us, sv, vfl, vfb, bv, kv,
--       stade, olympique, athletic, atletico, deportivo, real, club, the,
--       de, del, la, le, los, el)
--   5) Compression des espaces multiples
--
-- Sévérité prévue : warn (faux positifs possibles sur des clubs distincts partageant
-- une racine géographique ou sémantique, ex: Real Madrid vs Real Sociedad).
--
-- Test passe si le SELECT est vide.

WITH normed AS (
    SELECT DISTINCT
        {{ key_column }}  AS k,
        {{ name_column }} AS raw,
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER(TRANSLATE(
                        {{ name_column }},
                        'éèêëàáâäîíìïôöòóùûüúçñÉÈÊËÀÁÂÄÎÍÌÏÔÖÒÓÙÛÜÚÇÑ',
                        'eeeeaaaaiiiioooouuuuucnEEEEAAAAIIIIOOOOUUUUCN'
                    )),
                    '[.\-''`_/]', ' ', 'g'
                ),
                '\b(fc|cf|ac|sc|as|ca|rc|us|sv|vfl|vfb|bv|kv|stade|olympique|athletic|atletico|deportivo|real|club|the|de|del|la|le|los|el)\b',
                '', 'g'
            ),
            '\s+', ' ', 'g'
        )) AS norm
    FROM {{ model }}
    WHERE {{ name_column }} IS NOT NULL
)
SELECT
    a.k    AS key_a,
    a.raw  AS name_a,
    b.k    AS key_b,
    b.raw  AS name_b,
    a.norm AS normalized
FROM normed a
JOIN normed b
  ON a.k < b.k
 AND a.norm = b.norm
 AND a.norm <> ''

{% endtest %}
