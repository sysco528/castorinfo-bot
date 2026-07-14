# Castor Info v2 — bot de dépêches d'actualité pour X

Média automatisé de dépêches françaises : collecte **RSS** (conforme aux règles
d'automatisation de X), extraction factuelle par IA, **seuils de publication codés
en Python**, double contrôle avant publication, idempotence, journal auditable.

Architecture issue de l'audit du 13/07/2026 (voir le rapport) — elle remplace la
v1 qui lisait les tweets d'autres comptes via un service de scraping tiers
(twitterapi.io), méthode interdite par les CGU de X.

## Pipeline

```
RSS (9 médias notés) → extraction factuelle (Claude, JSON strict)
  → scoring + politique sensible (code) → rédaction (Claude)
  → contrôle secrétaire de rédaction (Claude) + filtres Python
  → carte-image de marque + publication idempotente sur X (ou DRY_RUN)
  → chaque soir ~19h : fil « 🦫 Le récap du jour »
```

**Cartes-images** : chaque dépêche est accompagnée d'une carte PNG générée à la
volée (police DejaVu bundlée dans `fonts/`, template CastorInfo). Média que le
compte possède → gain de portée, zéro risque de droits. Si l'upload échoue
(niveau d'accès API insuffisant), la dépêche part quand même **en texte seul**
(repli automatique, jamais de perte). Désactivable via `CARTES_ACTIVES` dans `bot.py`.

**Récap du soir** : au premier run à partir de 19h (heure de Paris), un fil
condense les 5-6 infos marquantes du jour. Hors quota, non soumis à la dédup.
Réglages : `HEURE_RECAP`, `MIN_DEPECHES_RECAP` dans `bot.py`.

**Auto-test image** : *Run workflow* → cocher `selftest` = tente un upload
d'image avec tes clés et s'arrête sans rien publier. À utiliser une fois pour
confirmer que ton niveau d'accès API autorise les images.

Règles clés (modifiables en tête de `bot.py`) :
- publication automatique si score ≥ 75 ; mise en attente 55–74 ; rejet < 55 ;
- un sujet **sensible** (décès, attentat, accusation nominative, élection, alerte
  sanitaire, catastrophe) n'est publié que confirmé : source fiabilité ≥ 90 avec
  confiance ≥ 85, **ou** 2 sources indépendantes ;
- « 🔴 ALERTE » impossible sans source très fiable (vérifié par le code, pas
  seulement par le modèle) ;
- similarité de formulation avec les sources plafonnée (trigrammes ≤ 0,55) ;
- interdits : liens, hashtags, mentions (un lien ferait aussi passer le coût du
  post de 0,015 $ à 0,20 $ sur l'API X pay-per-use) ;
- plafonds : 2 dépêches/passage, 10/jour (montée en charge progressive).

## Déploiement (première fois)

1. **Suspendre l'ancien bot** s'il tourne encore : *Actions → Castor Info Bot →
   … → Disable workflow*.
2. Pousser/téléverser ces fichiers sur la branche `main` :
   `bot.py`, `requirements.txt`, `.github/workflows/bot.yml`, `README.md`
   (garder `state.json` existant : la v2 le migre automatiquement).
3. Vérifier les **secrets** (Settings → Secrets and variables → Actions) :
   `ANTHROPIC_API_KEY`, `X_API_KEY`, `X_API_SECRET`, `X_ACCESS_TOKEN`,
   `X_ACCESS_SECRET`. Le secret `TWITTERAPI_KEY` n'est **plus utilisé** :
   le supprimer, et résilier le compte twitterapi.io.
4. **Test à blanc** : *Actions → Castor Info Bot → Run workflow* en laissant
   `dry_run = true`. Lire les logs : les dépêches s'affichent avec `🧪 [DRY_RUN]`
   sans être publiées. Recommencer sur 24–48 h et relire `journal.ndjson`.
5. Réactiver le cron (il tourne dès que le workflow est activé). Les runs
   planifiés publient réellement ; seul le lancement manuel propose `dry_run`.

## Exploitation

| Besoin | Geste |
|---|---|
| **Arrêt d'urgence** | Créer un fichier `STOP` (vide) à la racine du dépôt — l'interface web GitHub suffit. Le run suivant s'arrête immédiatement (run rouge = rappel visuel). Supprimer le fichier pour reprendre. |
| Simulation | *Run workflow* avec `dry_run = true` (les simulations consomment le quota du jour dans `state.json` — normal, c'est une répétition générale ; supprimer les lignes correspondantes du compteur si besoin). |
| Échec technique | Le run devient **rouge** et GitHub notifie par e-mail (Settings → Notifications → Actions). Plus de `sys.exit(0)` silencieux. |
| Journal des décisions | `journal.ndjson` — une ligne JSON par événement : `publie`, `attente`, `rejeter`, `doublon_bloque`, `controle`, `echec_*`… C'est la matière première des futurs rapports hebdo. |
| Ajouter/retirer une source | Éditer `FLUX_RSS` dans `bot.py` (url, nom, fiabilité 0-100, thème). La fiabilité pèse dans le score et dans la règle de confirmation des sujets sensibles. |
| Monter en cadence | `MAX_POSTS_PAR_JOUR` : 10 → 20 (semaine 3-4) → 30 ensuite. Rappel coût X : 0,015 $/post. |

## Feuille de route monétisation (rappel de l'audit)

Critères vérifiés le 13/07/2026 : Premium actif, compte ≥ 3 mois, 5 M
d'impressions organiques/3 mois, 500 abonnés vérifiés, 2FA, profil complet,
Stripe + vérification d'identité. Actions immédiates gratuites : 2FA sur X et
GitHub, bio/photo/bannière, label « Automated by » (paramètres du compte X).
Le partage de revenus X restera un complément : construire en parallèle la
newsletter (les fiches factuelles du journal sont déjà le brouillon quotidien).

## Limites connues / TODO

- Ajouter des flux « sources primaires » (vigilance Météo-France, vie-publique,
  communiqués officiels) quand des URL RSS stables sont identifiées.
- Récupérer les métriques des posts via l'API X dans le journal (KPI réels).
- Rapport hebdomadaire automatique (issue GitHub) à partir de `journal.ndjson`.
- Le cron GitHub reste non garanti (constaté : gros retards possibles) — la v2
  n'a plus de fenêtre de 45 min, donc un retard ne perd plus d'actualité, mais
  la latence de publication en dépend ; surveiller, et envisager un ordonnanceur
  externe si la réactivité devient un argument commercial.
