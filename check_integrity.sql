-- Vérification de l'intégrité (L6)
SELECT 'Ventes sans temps' as Test, COUNT(*) FROM dwh_mexora.fait_ventes f LEFT JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date WHERE t.id_date IS NULL
UNION ALL
SELECT 'Ventes sans produit', COUNT(*) FROM dwh_mexora.fait_ventes f LEFT JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk WHERE p.id_produit_sk IS NULL;