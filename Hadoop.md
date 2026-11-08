# Hadoop sous Ubuntu
## Clusters, HDFS, YARN, MapReduce et programmation en Python
## NOM: GRAH GRACE SEPHORA DIVINE
## PROF: DR BAKAYOKO
### Partie 1 - Les clusters : définition et présentation

#### 1. Concepts Clés à retenir
* **Qu'est-ce qu'un Cluster ?** C'est une grappe de serveurs (appelés nœuds) indépendants connectés par un réseau rapide. Ils fonctionnent ensemble sous la coordination d'un logiciel pour apparaître comme un système unique.
* **La Scalabilité Horizontale (Scale-out) :** C'est le pilier d'Hadoop. Au lieu de rendre une seule machine plus puissante et coûteuse (Scale-up), on ajoute des serveurs standards (*commodity hardware*) au cluster pour augmenter sa capacité de manière linéaire et quasi illimitée.
* **Le principe de Localité du calcul :** Pour éviter de saturer le réseau avec des téraoctets de données, Hadoop déplace le programme (très léger) là où les blocs de données sont physiquement stockés (le traitement va à la donnée).

#### 2. Les Modules Fondamentaux d'Hadoop
L'architecture repose sur une séparation stricte entre le stockage et le calcul :

| Composant | Module | Rôle Principal | Démon Maître | Démon Esclave |
| :--- | :--- | :--- | :--- | :--- |
| **Stockage** | HDFS | Découpe les fichiers en blocs (128 Mo par défaut) et gère leur réplication sur le cluster. | NameNode | DataNode |
| **Gestion** | YARN | Orchestre et distribue les ressources matérielles (CPU, RAM) sous forme de conteneurs. | ResourceManager | NodeManager |
| **Calcul** | MapReduce | Modèle de programmation permettant l'exécution parallèle des calculs via les étapes de Map et de Reduce. | ApplicationMaster *(un par job)* | Container *(Exécuteur)* |

---

### Partie 2 : Installation et déploiement du cluster pseudo-distribué

#### 1. Description de l'architecture déployée
L'installation a été réalisée en mode pseudo-distribué. Tous les démons Hadoop s'exécutent sur une seule et même instance virtuelle Ubuntu, mais chacun tourne dans un processus Java indépendant. L'environnement a été configuré avec les ressources recommandées, soit 4 Go de RAM et 20 Go d'espace disque (Page 1).

#### 2. Validation du démarrage (Preuve de fonctionnement)
Après avoir formaté le NameNode avec la commande `hdfs namenode -format` et démarré les services avec `start-dfs.sh` et `start-yarn.sh`, l'état du cluster a été vérifié à l'aide de l'utilitaire `jps`.

L'exécution de la commande affiche de manière stable les 5 démons fondamentaux actifs avec leurs numéros de processus réels :
* **NameNode :** 15937
* **DataNode :** 16058
* **SecondaryNameNode :** 16237
* **ResourceManager :** 16488
* **NodeManager :** 16619

#### 3. Note technique sur les difficultés résolues 
* **Migration vers Multipass (HCS) :** Suite à des échecs répétés de connexion réseau sous VirtualBox, l'environnement a été basculé avec succès sur Multipass en utilisant le pilote moderne de Windows Microsoft Hyper-V Compute System (HCS). Cette solution a permis d'obtenir une excellente stabilité réseau et une allocation stricte des 4 Go de RAM nécessaires sans plantage de l'hôte.
* **Correction de la configuration XML (core-site.xml) :** Un problème de syntaxe XML (*Multiple roots exception*) a été détecté lors de l'analyse initiale du fichier par Hadoop. Le fichier a été corrigé en nettoyant les balises redondantes pour n'avoir qu'une racine `<configuration>` unique et valide, résolvant ainsi l'erreur de parsing lors des requêtes HDFS.
* **Incident constaté lors de la validation par la commande jps :** Contrairement au résultat théorique attendu dans le livret de TP (Page 11), l'exécution initiale de la commande `jps` n'affichait pas les 5 démons Hadoop. Seul le processus de contrôle Jps lui-même était visible, indiquant qu'aucun démon Java ne restait actif en arrière-plan. Ce problème lié aux variables d'environnement et aux syntaxes XML a ensuite été corrigé avec succès pour obtenir le démarrage stable décrit ci-dessus.


---

### Partie 3 : Manipulation de HDFS

#### Exercice 1 : Premiers pas avec HDFS

* **Question 1 : Combien de blocs le fichier poeme.txt occupe-t-il ? Pourquoi ?**
  * **Réponse :** Le fichier `poeme.txt` occupe **1 seul bloc** (référencé sous l'identifiant unique `blk_1073741825` dans le rapport FSCK). La taille par défaut d'un bloc HDFS étant de 128 Mo et le fichier `poeme.txt` ne pesant que 85 octets, Hadoop lui alloue un unique bloc, qui constitue l'unité minimale de stockage.

* **Question 2 : Retrouvez le fichier dans l'interface web du NameNode (Utilities → Browse the file system).**
  * **Réponse :** L'interface web du NameNode est active et accessible à l'adresse `http://localhost:9870`. En allant dans l'onglet *Utilities -> Browse the file system*, on peut naviguer dans l'arborescence distribuée et constater la présence du fichier `poeme.txt` au chemin exact : `/user/hadoop/tp/input/poeme.txt`. On y retrouve ses métadonnées : une taille de 85 Bytes, un facteur de réplication de 1, et l'appartenance à l'utilisateur `hadoop` du groupe `supergroup`.

* **Question 3 : Quel serait le nombre de blocs d'un fichier de 1 Go avec la taille de bloc par défaut ? Et l'espace disque utilisé avec une réplication de 3 ?**
  * **Réponse :**
    * **Nombre de blocs pour 1 Go :** 1 Go correspond à 1024 Mo. La taille d'un bloc par défaut étant de 128 Mo, le calcul est : $1024 \text{ Mo} / 128 \text{ Mo} = \mathbf{8 \text{ blocs}}$.
    * **Espace disque utilisé avec une réplication de 3 :** Le facteur de réplication de 3 implique que chaque bloc est copié en 3 exemplaires physiques distincts. Le volume total de données physiques stockées sur les disques durs est de : $8 \text{ blocs} \times 128 \text{ Mo} \times 3 = 3072 \text{ Mo} = \mathbf{3 \text{ Go}}$.

---

### Partie 4 : Le modèle MapReduce

#### 1. Ce qu'implique ce modèle pour le développeur
Le grand intérêt du modèle MapReduce est qu'il décharge complètement le développeur de la complexité de la parallélisation et de la gestion du réseau. L'effort se résume à concevoir deux fonctions logiques simples :
* **Le Mapper (Le Trieur) :** Il prend les lignes de données brutes et les transforme en paires intermédiaires (clé, valeur). Tout l'enjeu est de choisir la bonne "clé", car c'est elle qui dictera comment Hadoop regroupera les données par la suite.
* **Le Reducer (L'Agrégateur) :** Il récupère les paires regroupées autour d'une même clé et exécute l'opération finale (somme, maximum, moyenne) pour générer le résultat.

#### 2. Le concept clé de la "Localité des données"
Il s'agit de déplacer le calcul vers les données, et non l'inverse. Transférer des fichiers de plusieurs téraoctets sur le réseau saturerait immédiatement la bande passante. Hadoop résout cela en envoyant nos petits scripts Python s'exécuter directement sur les nœuds esclaves (`DataNodes`) qui possèdent déjà les blocs physiques du fichier.

#### 3. L'importance cruciale de la phase de "Shuffle & Sort"
C'est la phase charnière du framework (gérée entièrement par Hadoop). Avant que les données n'arrivent sur l'entrée standard du Reducer, Hadoop trie et regroupe toutes les clés identiques à travers le réseau. C'est grâce à ce tri automatique que le code Python peut supposer que toutes les lignes concernant une même clé arriveront de manière consécutive.

#### 4. Bilan sur les forces et limites du modèle
Bien que MapReduce offre un passage à l'échelle quasi-linéaire et une tolérance aux pannes totalement automatisée, il possède des limites. Sa latence élevée le rend obsolète pour le traitement en temps réel. De plus, l'obligation d'écrire les résultats intermédiaires sur le disque dur à chaque étape le rend très lourd pour les algorithmes itératifs, raison pour laquelle il est aujourd'hui souvent complété ou remplacé par Apache Spark pour le calcul en mémoire vive (RAM).

---

### Partie 5 - Pratique : MapReduce en Python

#### Exercice 2 : WordCount en Python

##### 1. Résultats officiels obtenus (Le Top 20)
Après exécution du Job distribué sur le texte complet du Tome I des Misérables, le tri informatique renvoie le classement exact suivant :

| Rang | Mot | Occurrences |
| :--- | :--- | :--- |
| 1 | de | 4 472 |
| 2 | il | 3 192 |
| 3 | la | 3 040 |
| 4 | et | 2 949 |
| 5 | le | 2 548 |
| 6 | l | 2 391 |
| 7 | à | 2 324 |
| 8 | un | 1 812 |
| 9 | les | 1 537 |
| 10 | que | 1 350 |
| 11 | une | 1 314 |
| 12 | d | 1 293 |
| 13 | qui | 1 278 |
| 14 | qu | 1 210 |
| 15 | en | 1 194 |
| 16 | était | 1 175 |
| 17 | dans | 1 134 |
| 18 | est | 1 130 |
| 19 | ce | 1 062 |
| 20 | des | 948 |

##### 2. Ce que l'on peut retenir et analyser (Analyse critique)
* **Prédominance des mots fonctionnels :** L'analyse textuelle montre que le sommet du dictionnaire est exclusivement occupé par des mots vides (*stop words*) : articles, prépositions et conjonctions. Le mot « de » domine largement avec 4 472 occurrences. Cela valide la pertinence statistique de la loi de Zipf dans les analyses linguistiques de gros volumes textuels.
* **Limites du nettoyage élémentaire :** Des caractères élidés isolés comme « l » (2 391), « d » (1 293) ou « qu » (1 210) apparaissent comme des mots pleins. Cela prouve que pour un projet de *Text Mining* en entreprise, l'étape de nettoyage du Mapper doit être enrichie pour filtrer ces coupures ou utiliser une liste de mots vides à ignorer.
* **Performance du cluster :** Bien que le texte contienne des milliers de mots, Hadoop (via YARN) a automatiquement géré la phase intermédiaire de *Shuffle & Sort* pour distribuer, trier et sérialiser les clés afin que le Reducer Python reçoive les lignes identiques de manière consécutive.

#### Exercice 3 : Chiffre d'affaires par ville

##### 1. Objectif
Analyser un fichier de transactions commerciales (`ventes.csv`) comprenant l'activité de plusieurs localités de Côte d'Ivoire afin d'extraire de manière distribuée le chiffre d'affaires global et le nombre de ventes par ville.

##### 2. Code et Résultats HDFS obtenus (part-00000)
La commande `hdfs dfs -cat /user/hadoop/tp/output_ventes/part-00000` renvoie le bilan comptable exact suivant :
```text
Abidjan         910000  3
Bouake          445000  2
Korhogo         40000   1
San-Pedro       260000  2
Yamoussoukro    540000  2
```

##### 3. Analyse des compteurs MapReduce
* **Map input records = 11 :** Le framework a lu les 11 lignes du fichier d'entrée (10 lignes de données + 1 ligne d'en-tête).
* **Map output records = 10 :** Le Mapper a correctement appliqué son filtre en ignorant la ligne d'en-tête `date,ville...` pour n'émettre que les 10 paires de transactions valides.
* **Reduce output records = 5 :** Après la phase automatique de tri et regroupement par clé (*Shuffle & Sort*), le Reducer a condensé le flux pour produire les statistiques finales des 5 villes uniques présentes dans le jeu de données.

#### Exercice 4 : Température maximale par année

##### Analyse technique des compteurs MapReduce
L'examen des logs YARN et des compteurs d'Hadoop permet de valider le comportement du cluster à chaque étape :
* **Map input records = 6 :** Le framework a lu l'intégralité des 6 lignes de relevés du fichier `meteo.txt`.
* **Map output records = 6 :** Le Mapper a extrait et émis 6 paires intermédiaires sous la forme `annee<TAB>temperature`.
* **Reduce input groups = 2 :** La phase intermédiaire de *Shuffle & Sort* d'Hadoop a correctement fusionné et trié les données autour de 2 clés uniques (l'année 2024 et l'année 2025).
* **Reduce output records = 2 :** Le Reducer a analysé chaque groupe pour n'émettre que la valeur maximale trouvée, produisant le fichier dictionnaire final de 2 lignes.

##### Conclusion de l'exercice
Cet exercice démontre la flexibilité du modèle MapReduce. Contrairement aux exercices précédents (WordCount ou chiffre d'affaires par ville) qui effectuaient des sommes accumulées, l'Exercice 4 prouve que la phase de réduction (*Reduce*) peut être programmée pour exécuter un algorithme de recherche de maximum.

Le script intègre également une conversion de type (`float(temperature)`) protégée par un bloc `try/except` en Python, ce qui garantit que les relevés météo corrompus ou textuels sont rejetés sans interrompre le traitement du cluster informatique.

#### Exercice 5 : Moyenne des notes par matière

* **Question : Peut-on utiliser le reducer de moyenne comme combiner ? Justifiez. (Indice : la moyenne des moyennes est-elle égale à la moyenne globale ?) Proposez une solution où le mapper émet matiere\<TAB\>note\<TAB\>1.**
  * **Réponse :** Non, on ne peut pas utiliser directement un reducer calculant une moyenne comme combiner.
  * **Justification :** L'opération mathématique de la moyenne n'est ni associative ni commutative lorsque les sous-groupes n'ont pas le même effectif (la moyenne des moyennes n'est pas égale à la moyenne globale). Par exemple, si un étudiant a 18/20 dans un premier bloc et que deux étudiants ont chacun 12/20 dans un second bloc, faire la moyenne des moyennes donnerait : (18 + 12) / 2 = 15, alors que la moyenne globale réelle est de : (18 + 12 + 12) / 3 = 14.

##### Solution technique et logique MapReduce
Pour que le traitement soit exact tout en permettant l'utilisation d'un combiner, le Mapper doit émettre trois informations : `matiere\tnote\t1`. De cette façon :
1. Le **Combiner** peut faire la somme partielle des notes et la somme partielle des compteurs (les coefficients) localement sur chaque nœud, ce qui optimise le trafic réseau.
2. Le **Reducer final** récupère ces sommes, cumule le total global des notes, le divise par le nombre total de coefficients, et calcule ainsi la moyenne globale exacte.

##### Analyse technique des compteurs MapReduce pour l'Exercice 5
En observant le résumé des métriques imprimé dans la console PowerShell, on valide le comportement du framework à chaque étape :
* **Map input records = 6 :** Hadoop a lu les 6 lignes du fichier d'entrée (les 5 lignes d'étudiants + la ligne d'en-tête du fichier CSV).
* **Map output records = 5 :** Le Mapper a correctement filtré l'en-tête grâce à l'instruction `if ligne.startswith("etudiant"): continue` pour n'émettre que les 5 paires valides vers le cluster.
* **Reduce input groups = 2 :** La phase de *Shuffle & Sort* d'Hadoop a fusionné et trié le flux de données autour de 2 clés uniques distinctes (les matières Hadoop et Maths).
* **Reduce output records = 2 :** Le Reducer a calculé la moyenne algébrique exacte pour chaque groupe de clés, générant le fichier final `part-00000` de 2 lignes.

##### Conclusion de l'exercice
Le succès de ce script met en évidence une contrainte architecturale majeure de la phase de Combiner en environnement Big Data. Les calculs non commutatifs ou non associatifs (comme une moyenne ou une médiane) ne peuvent pas utiliser de fonction de réduction standard en tant que Combiner direct. Il est donc nécessaire de conserver séparément les sommes et les coefficients pour n'effectuer la division qu'au stade final du Reducer.

#### Exercice 6 : Index inversé

---

### Partie 6 : Dépannage et passage à un vrai cluster

#### 6.1 Synthèse du Dépannage (Problèmes fréquents)
L'exploitation pratique de ce TP sous Ubuntu a permis de se confronter aux réalités de l'administration et du débogage d'un cluster Hadoop :
* **L'environnement Java (JAVA_HOME) :** C'est un piège classique. Même si la variable est déclarée dans la session utilisateur (`.bashrc`), elle doit obligatoirement être inscrite en dur dans le fichier `hadoop-env.sh`. Sans cela, les démons démarrés à distance via SSH ne peuvent pas localiser Java, ce qui bloque le lancement du cluster.
* **La sécurité des répertoires de sortie (Output directory already exists) :** Hadoop interdit nativement d'exécuter un Job MapReduce si le répertoire de destination existe déjà sur HDFS. Cette sécurité cruciale évite d'écraser par accident des données issues de calculs précédents. Un nettoyage préalable (`hdfs dfs -rm -r`) est obligatoire avant de relancer un traitement.
* **Erreur de script (subprocess failed with code 1) :** Ce message indique qu'une erreur de syntaxe ou un caractère parasite s'est glissé dans les scripts Python. La méthode de validation consiste à tester les scripts localement via un tube Linux (`cat | mapper | sort | reducer`) avant de les soumettre à YARN.

#### 6.2 Méthodologie de Passage à l'Échelle (Scale-out)
Pour faire migrer l'environnement d'apprentissage (mode pseudo-distribué sur une seule machine) vers un cluster de production réel multi-nœuds (mode totalement distribué), l'architecture informatique doit être reconfigurée selon les principes suivants :

1. **Uniformisation du parc (Étape 1) :** Installer exactement les mêmes versions de Java 11 et de Hadoop 3.3.6, sur les mêmes chemins absolus, sur l'ensemble des serveurs du réseau.
2. **Résolution réseau (Étape 2) :** Renseigner les adresses IP et les noms de chaque serveur (ex: `master`, `worker1`, `worker2`) dans le fichier système `/etc/hosts` de toutes les machines pour permettre une communication par alias clairs plutôt que par IP brutes.
3. **Confiance SSH et automatisation (Étape 3) :** Déployer la clé SSH publique du nœud maître sur tous les esclaves (`ssh-copy-id`). Cela permet aux scripts d'administration du Maître de démarrer et superviser à distance les démons esclaves sans demande de mot de passe.
4. **Fichiers de configuration maîtres (Étapes 4, 5 & 6) :**
   * Dans `core-site.xml`, l'adresse du système de fichiers virtuel bascule de localhost vers le nœud maître : `hdfs://master:9000`.
   * Dans `yarn-site.xml`, la propriété `yarn.resourcemanager.hostname` doit être configurée sur `master` afin de centraliser l'ordonnancement des calculs.
   * Dans `hdfs-site.xml`, la valeur de réplication (`dfs.replication`) doit être augmentée à 2 ou 3. Les blocs de données seront ainsi dupliqués sur des serveurs physiques distincts, assurant la tolérance aux pannes native d'HDFS.
5. **Déclaration des Esclaves (Étape 7) :** Recenser l'ensemble des nœuds de calcul dans le fichier `$HADOOP_HOME/etc/hadoop/workers` (un nom d'hôte par ligne).

##### Validation du Cluster Réel (Étape 8)
Après avoir synchronisé les configurations, formaté le NameNode sur le Maître et lancé les scripts de démarrage (`start-dfs.sh` et `start-yarn.sh`), la validation se fait de deux manières :
* **En ligne de commande :** L'instruction `hdfs dfsadmin -report` doit lister l'ensemble des esclaves connectés sous la métrique *Live datanodes*.
* **Via l'interface Web YARN :** La page `http://master:8088/cluster/nodes` doit afficher l'état actif, les processeurs (vCores) et la mémoire vive disponible sur chaque NodeManager du réseau.

---

### Réponses aux questions de synthèse 

1. **Donnez une définition d'un cluster et citez trois de ses avantages.**
   * **Réponse :** Un cluster est un ensemble d'ordinateurs indépendants, appelés nœuds, reliés par un réseau rapide et coordonnés par un logiciel pour fonctionner comme un seul système unique aux yeux de l'utilisateur.
   * **Trois avantages :**
     * *Puissance de calcul :* Permet de traiter de gros volumes de données en parallèle (scalabilité horizontale).
     * *Tolérance aux pannes (Haute disponibilité) :* Si un nœud tombe en panne, les autres prennent le relais.
     * *Optimisation des coûts :* Utilisation de matériel informatique standard (*commodity hardware*) au lieu de supercalculateurs onéreux.

2. **Quelle différence faites-vous entre scalabilité verticale et horizontale ? Laquelle Hadoop exploite-t-il ?**
   * **Réponse :** La scalabilité verticale (*Scale-up*) consiste à augmenter la puissance d'une seule machine (plus de CPU ou de RAM), ce qui atteint rapidement un plafond matériel très coûteux. La scalabilité horizontale (*Scale-out*) consiste à ajouter de nouvelles machines ordinaires en réseau au cluster, offrant une capacité d'évolution quasi-illimitée. Hadoop exploite exclusivement la scalabilité horizontale.

3. **Quel est le rôle du NameNode ? Pourquoi constitue-t-il un point sensible du cluster ?**
   * **Réponse :** Le NameNode est le nœud maître du stockage HDFS. Il gère l'espace de noms (l'arborescence des dossiers et les droits) et sait précisément sur quels nœuds esclaves (`DataNodes`) se trouve chaque bloc de fichier. Il ne stocke aucune donnée utilisateur.
   * **Sensibilité :** Il constitue un point sensible unique de défaillance (*Single Point of Failure*) car s'il s'arrête, tout le cluster devient aveugle et inutilisable, car l'emplacement des données est perdu.

4. **Le Secondary NameNode est-il une copie de secours du NameNode ? Expliquez.**
   * **Réponse :** Non, le Secondary NameNode n'est pas un NameNode de secours. Son rôle est uniquement d'alléger le travail du NameNode principal en fusionnant périodiquement le journal des modifications (*edits*) avec l'image globale du système de fichiers (*fsimage*). Si le NameNode principal tombe en panne, le Secondary NameNode ne peut pas le remplacer automatiquement pour maintenir le cluster actif.

5. **Qu'est-ce que la localité des données et pourquoi est-elle importante ?**
   * **Réponse :** La localité des données consiste à déplacer le calcul vers les données. Au lieu de transférer des fichiers gigantesques à travers le réseau vers un programme central, Hadoop envoie le code du programme s'exécuter directement sur le nœud esclave qui possède physiquement le bloc de données. Cela est important car cela évite de saturer la bande passante du réseau local, qui est la principale limite dans les traitements Big Data.

6. **Décrivez dans l'ordre les phases d'un job MapReduce et indiquez celles écrites par le développeur.**
   * **Réponse :** Un job traverse les 7 phases suivantes dans l'ordre chronologique :
     1. Input / Split *(Hadoop)*
     2. **Map** *(Développeur)*
     3. **Combine** *(Développeur - Optionnelle)*
     4. Partition *(Hadoop)*
     5. Shuffle & Sort *(Hadoop)*
     6. **Reduce** *(Développeur)*
     7. Output *(Hadoop)*

7. **Pourquoi le reducer Python peut-il supposer que les lignes d'une même clé sont consécutives ?**
   * **Réponse :** C'est le rôle fondamental de la phase intermédiaire de *Shuffle & Sort* d'Hadoop. Avant d'envoyer les lignes sur l'entrée standard (`sys.stdin`) du script reducer, Hadoop trie et regroupe l'intégralité des paires intermédiaires par clé. Cela garantit mathématiquement que toutes les lignes partageant une même clé arrivent les unes à la suite des autres.

8. **Dans quels cas un combiner est-il utilisable ? Donnez un exemple où il ne l'est pas directement.**
   * **Réponse :** Un combiner (ou mini-reducer local) est utilisable uniquement si l'opération d'agrégation finale est commutative et associative (comme une addition ou la recherche d'un maximum).
   * **Exemple valide :** Le calcul d'un total ou d'un chiffre d'affaires.
   * **Exemple non valide directement :** Le calcul d'une moyenne de notes (Exercice 5). La moyenne des moyennes n'étant pas égale à la moyenne globale, on ne peut pas utiliser le reducer de moyenne directement comme combiner sans modifier la structure des données émises par le mapper.

9. **Que se passe-t-il si un nœud tombe en panne pendant l'exécution d'une tâche Map ?**
   * **Réponse :** Hadoop considère la panne comme un événement normal. Le gestionnaire de ressources YARN (via l'ApplicationMaster) détecte la perte du signal de vie (*heartbeat*) du nœud en panne. Grâce à la réplication, Hadoop recherche une autre copie du bloc de données disponible sur un nœud sain et y relance automatiquement la tâche Map échouée, de manière totalement transparente pour l'utilisateur.

10. **Citez deux limites de MapReduce et un outil de l'écosystème qui y répond.**
    * **Réponse :**
      * *Première limite :* Une latence élevée qui rend le modèle inadapté aux analyses de données en temps réel.
      * *Deuxième limite :* Une grande lourdeur pour les algorithmes et traitements itératifs (obligation d'écrire et relire sur les disques durs entre chaque job).
      * *L'outil de l'écosystème qui y répond :* **Apache Spark**, qui exécute les calculs directement en mémoire vive (RAM), éliminant les écritures disques intermédiaires.
