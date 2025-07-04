from collections import defaultdict
import csv

#cherche les passages qui répondent à au moins une requête
#crée un fichier au format id_passage passage id_req requete_repondue
def creer_fichier_passages_1():
    print("création du fichier de passages et de requêtes...")
    # Chemins vers les fichiers
    collection_path = 'data/collection.tsv'
    queries_path = 'data/queries.train.tsv'
    qrels_path = 'data/qrels.train.tsv'

    # Dictionnaire pour stocker les passages pertinents pour chaque requête
    relevant_passages = defaultdict(list)
    queries = {}
    passages = {}

    # Charger les requêtes
    with open(queries_path, 'r', encoding='utf-8') as f:
        for line in f:
            qid, query = line.strip().split('\t')
            queries[qid] = query

    # Charger les passages pertinents (qrels)
    with open(qrels_path, 'r', encoding='utf-8') as f:
        for line in f:
            qid, _, pid, rel = line.strip().split('\t')
            if int(rel) > 0:
                relevant_passages[pid].append(qid)

    # Charger tous les passages
    with open(collection_path, 'r', encoding='utf-8') as f:
        for line in f:
            pid, text = line.strip().split('\t')
            passages[pid] = text

    # Créer une liste des passages qui répondent à au moins une requête
    with open('data/passages_queries.tsv', 'w', encoding='utf-8') as out:
        for pid, qids in relevant_passages.items():
            for qid in qids:
                if pid in passages and qid in queries:
                    out.write(f"{pid}\t{passages[pid]}\t{qid}\t{queries[qid]}\n")
    print("fichier créé")

#construire le mapping des passages avec les id de passages
def construire_dict_passages(collection):
    print("construire_dict_passages")
    dict_passages = {}
    with open(collection, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            #print(line)
            l = line.strip().split('\t', maxsplit=1) #tabulations et pas espaces, snif
            if len(l) == 2:
                dict_passages[l[0]] = l[1]
    return dict_passages

#construire le mapping des titres avec les id de passages
def construire_dict_titres_et_ids(fichier_titres):
    print("construire_dict_titres_et_ids")
    ids=[]
    dict_titre = {}
    with open(fichier_titres, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            #print(line)
            l = line.strip().split('\t')
            if len(l) >= 2:
                #print("l[1] : ",l[1])
                id=l[0]
                ids.append(id)
                dict_titre[id] = l[1]
    #print("ids : ",ids)
    return ids,dict_titre

#crée un dictionnaire id_passage:titre_passage
def charger_titres(fichier_titres):
    print("charger titres")
    titres = {}
    with open(fichier_titres, 'r', encoding='utf-8') as f:
        for ligne in csv.reader(f, delimiter='\t'):
            if len(ligne) >= 2:
                titres[ligne[0]] = ligne[1].strip()
    return titres

#construit un dictionnaire id_req:texte_requete
def construire_dict_req(fichier_queries):
    dict_req = {}
    with open(fichier_queries, 'r', encoding='utf-8') as f:
        for line in f:
            l=line.strip().split('\t')
            id_req=l[0]
            req=l[1]
            dict_req[id_req]=req
    return dict_req

#construit le fichier comprenant id_passage,titre,passage
def construit_fichier_passages(fichier_titres, fichier_titres_passages, dict_passages,dict_titres):
    print("construit_fichier_passages")
    with open(fichier_titres, 'r', encoding='utf-8') as fichier_in,open(fichier_titres_passages, 'w', encoding='utf-8') as fichier_out:
        for line in fichier_in:
            #print(line)
            colonnes = line.strip().split('\t')
            id_passage = colonnes[0]
            titre = colonnes[1]
            passage = dict_passages.get(id_passage)
            #print("id_passage : ",id_passage)
            #print("titre : ",titre)
            #print("passage : ",passage)

            fichier_out.write(f"{id_passage}\t{titre}\t{passage}\n")
            #print(f"Recherche passage pour id: {id_passage}")
            #print(f"Trouvé dans dict_passages ? {'Oui' if id_passage in dict_passages else 'Non'}")

#Lit un fichier TSV avec des lignes contenant : id \t titre \t passage + gère les retours à la ligne
#renvoie un tableau d'id de passages, un dictionnaire id:titre, un dictionnaire id:passage
#à appeler une fois que fichier_titres_passages a été construit
def charger_ids_titres_passages(fichier_titres_passages):
    print("charger_ids_titres_passages")
    ids=[]
    titres = {}
    passages = {}
    with open(fichier_titres_passages, 'r', encoding='utf-8') as f:
        for ligne in csv.reader(f, delimiter='\t'):
            id=ligne[0]
            ids.append(id)
            titres[id] = ligne[1].strip()
            passages[id] = ligne[2].strip()
    return ids,titres,passages

def construire_dict_passages_repondant_au_moins_1(fichier_passages_req,fichier_titres):
    print("construire_dict_passages")
    dict_passages={}
    dict_req={}
    dict_passages_req={}
    dict_titres={}
    
    #construire les dictionnaires
    print("construction dictionnaires")
    with open(fichier_passages_req, "r", encoding="utf-8") as f:
        for line in f:
            #print(line)
            l=line.strip().split('\t')
            id_passage=l[0]
            passage=l[1]
            id_req=l[2]
            req=l[3]
            dict_passages[id_passage]=passage
            dict_req[id_req]=req
            dict_passages_req[id_passage]=req

    #construire le dictionnaire des titres de passages
    with open(fichier_titres, "r", encoding="utf-8") as f:
        for line in f:
            #print(line)
            l=line.strip().split('\t')
            #print(l)
            id_passage=l[0]
            titre=l[1]
            dict_titres[id_passage]=titre
    """
    print("contenu dict_passages : ")
    for i, (key, value) in enumerate(dict_passages.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")

    print("contenu dict_req : ")
    for i, (key, value) in enumerate(dict_req.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")

    print("contenu dict_passages_req : ")
    for i, (key, value) in enumerate(dict_passages_req.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")

    print("contenu dict_titres : ")
    for i, (key, value) in enumerate(dict_titres.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")
    """
    return dict_passages,dict_req,dict_passages_req,dict_titres

def construire_dict_passages_repondant_au_moins_1_2(fichier_passages_req,fichier_titres):
    print("construire_dict_passages")
    ids=[]
    dict_passages={}
    dict_req={}
    dict_passages_req=defaultdict(list)
    dict_titres_intermediaire={}
    
    #construire les dictionnaires
    print("construction dictionnaires")
    with open(fichier_passages_req, "r", encoding="utf-8") as f:
        for line in f:
            #print(line)
            l=line.strip().split('\t')
            id_passage=l[0]
            passage=l[1]
            id_req=l[2]
            req=l[3]
            ids.append(id_passage)
            dict_passages[id_passage]=passage
            dict_req[id_req]=req
            dict_passages_req[id_passage].append(req)

    #construire le dictionnaire des titres de passages qui répondent à au moins une requête
    #dictionnaire pour tous les passages
    with open(fichier_titres, "r", encoding="utf-8") as f:
        for line in f:
            #print(line)
            l=line.strip().split('\t')
            #print(l)
            id_passage=l[0]
            titre=l[1]
            #vérifier que l'id soit celui d'un passage qui réponde à au moins une requête ??
            #if(id_passage in dict_passages.keys()):
                #print("Passage ici")
            dict_titres_intermediaire[id_passage]=titre
    #dictionnaire que pour les passages qui répondent à au moins 1 requête
    dict_titres={}
    for id_passage in dict_passages.keys():
        dict_titres[id_passage]=dict_titres_intermediaire.get(id_passage)

    print("contenu dict_passages : ")
    for i, (key, value) in enumerate(dict_passages.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")

    print("contenu dict_req : ")
    for i, (key, value) in enumerate(dict_req.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")

    print("contenu dict_passages_req : ")
    for i, (key, value) in enumerate(dict_passages_req.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")

    print("contenu dict_titres : ")
    for i, (key, value) in enumerate(dict_titres.items()):
        if i >= 5:
            break
        print(f"{key}: {value}")
        
    return ids,dict_passages,dict_req,dict_passages_req,dict_titres