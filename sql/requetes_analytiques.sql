-- ============================================================
-- REQUÊTES ANALYTIQUES — Pipeline Mobile Money CI
-- ============================================================

-- REQUÊTE 1 : Parts de marché par opérateur
SELECT
    o.nom_operateur,
    COUNT(f.id_transaction)                          AS nb_transactions,
    SUM(f.montant_fcfa)                              AS volume_total_fcfa,
    ROUND(SUM(f.montant_fcfa) * 100.0 / 
          SUM(SUM(f.montant_fcfa)) OVER (), 2)       AS part_marche_pct,
    ROUND(AVG(f.montant_fcfa), 0)                    AS montant_moyen
FROM faits_transactions f
JOIN dim_operateur o ON f.id_operateur = o.id_operateur
WHERE f.statut = 'Succès'
GROUP BY o.nom_operateur
ORDER BY volume_total_fcfa DESC;

-- REQUÊTE 2 : Tendances mensuelles
SELECT
    d.mois, d.nom_mois,
    COUNT(f.id_transaction)        AS nb_transactions,
    SUM(f.montant_fcfa)            AS volume_fcfa,
    ROUND(AVG(f.montant_fcfa), 0)  AS montant_moyen,
    COUNT(CASE WHEN f.statut = 'Échec' THEN 1 END) AS nb_echecs
FROM faits_transactions f
JOIN dim_date d ON f.id_date = d.id_date
GROUP BY d.mois, d.nom_mois
ORDER BY d.mois;

-- REQUÊTE 3 : Taux d'échec par opérateur
SELECT
    o.nom_operateur,
    COUNT(f.id_transaction)                             AS total_transactions,
    COUNT(CASE WHEN f.statut = 'Succès'     THEN 1 END) AS nb_succes,
    COUNT(CASE WHEN f.statut = 'Échec'      THEN 1 END) AS nb_echecs,
    COUNT(CASE WHEN f.statut = 'En attente' THEN 1 END) AS nb_en_attente,
    ROUND(
        COUNT(CASE WHEN f.statut = 'Échec' THEN 1 END)
        * 100.0 / COUNT(f.id_transaction), 2
    )                                                    AS taux_echec_pct
FROM faits_transactions f
JOIN dim_operateur o ON f.id_operateur = o.id_operateur
GROUP BY o.nom_operateur
ORDER BY taux_echec_pct DESC;

-- REQUÊTE 4 : Top zones par volume
SELECT
    z.nom_zone, z.region,
    COUNT(f.id_transaction)                    AS nb_transactions,
    SUM(f.montant_fcfa)                        AS volume_fcfa,
    ROUND(SUM(f.montant_fcfa) * 100.0 /
          SUM(SUM(f.montant_fcfa)) OVER (), 2) AS part_volume_pct,
    ROUND(AVG(f.montant_fcfa), 0)              AS montant_moyen
FROM faits_transactions f
JOIN dim_zone z ON f.id_zone_exp = z.id_zone
WHERE f.statut = 'Succès'
GROUP BY z.nom_zone, z.region
ORDER BY volume_fcfa DESC
LIMIT 10;

-- REQUÊTE 5 (AVANCÉE) : Classement mensuel avec CTE + RANK()
WITH volumes_mensuels AS (
    SELECT
        d.nom_mois, d.mois,
        o.nom_operateur,
        COUNT(f.id_transaction) AS nb_transactions,
        SUM(f.montant_fcfa)     AS volume_fcfa
    FROM faits_transactions f
    JOIN dim_date d      ON f.id_date      = d.id_date
    JOIN dim_operateur o ON f.id_operateur = o.id_operateur
    WHERE f.statut = 'Succès'
    GROUP BY d.mois, d.nom_mois, o.nom_operateur
)
SELECT
    nom_mois, nom_operateur, volume_fcfa, nb_transactions,
    RANK() OVER (
        PARTITION BY mois
        ORDER BY volume_fcfa DESC
    ) AS rang_mensuel
FROM volumes_mensuels
ORDER BY mois, rang_mensuel;
