from datetime import datetime
import time
import creer_fichier_dict as cfd
from tqdm import tqdm
import dice_score as dc
import os
import json
from pyserini.search.lucene import LuceneSearcher
from multiprocessing import Pool, cpu_count

#on suppose les passages uniques
def recup_id_avec_valeur(passage_a_chercher,dict_passage):
    for id,passage in dict_passage.items():
        if(passage==passage_a_chercher):
            return id

#appelée en parallèle pour trouver le plus passage le plus proche d'un autre
def rechercher_voisin(id, passage, k, index_path, fichier_premiers):
    searcher = LuceneSearcher(index_path)
    searcher.set_bm25()
    self_found = False 
    nb_not_self=0
    id_not_self = 0
    hits = searcher.search(passage, k + 1)
    for hit in hits:
        #vérifier que le plus proche soit le passage lui-même + garder le log
        if hit.docid == id:
            self_found = True
            continue  # skip self
        #récupérer le premier passage qui ne soit pas hit lui-même et le renvoyer
        else : #if hit.docid != id 
            if(not self_found):
                id_not_self=hit.docid
            return [id, hit.docid]
        
    #si self non trouvé, log "data/similaires/premiers_3000.log"
    if not self_found:
        f=open(fichier_premiers)
        print(f"[WARN] Le passage {id} n'est pas retourné comme son propre top résultat.")
        #structure : id_passage, id_premier
        f.write(f"{id} {id_not_self}\n")
    return None

def calculer_voisins_pyserini(dict_passage, k, index_path, fichier_premiers):
    print("Recherche avec Pyserini (BM25) + multiprocessing...")

    #arguments pour chaque tâche
    args = [(id, passage, k, index_path,fichier_premiers) for id, passage in dict_passage.items()]
    #pour utiliser tous les coeurs dispo
    with Pool(processes=cpu_count()) as pool: 
        #resultats = pool.starmap(rechercher_voisin, args)
        resultats = list(tqdm(pool.starmap(rechercher_voisin, args), total=len(args), desc="Recherche BM25"))

    voisins = []
    ids_deja_vus = set()
    for res in resultats:
        if res:
            id1, id2 = res
            pair = tuple(sorted([id1, id2]))
            if pair not in ids_deja_vus:
                ids_deja_vus.add(pair)
                voisins.append([id1, id2])
    return voisins

def convertir_collection_en_jsonl(dict_passage, jsonl_path):
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for id, passage in dict_passage.items():
            json.dump({"id": id, "contents": passage}, f)
            f.write("\n")

def indexer_passages_pyserini(jsonl_dir, index_dir):
    print("Indexation avec Pyserini...")
    os.system(
        f"python -m pyserini.index.lucene "
        f"--collection JsonCollection "
        f"--input {jsonl_dir} "
        f"--index {index_dir} "
        f"--generator DefaultLuceneDocumentGenerator "
        f"--threads 4 "
        f"--storePositions --storeDocvectors --storeRaw"
    )

def traitement(fichier_titres,fichier_log_similaires, fichier_logs_premiers,collection,fichier_titres_passages,fichier_passages_requetes,nb_passages,creer_titres_passages,indexer,taille_titres):
    start_time1 = time.time()
    index_path = "pyserini_index"
    jsonl_file = "pyserini_data/passages.jsonl"

    if indexer:
        batch_paths = []
        print("fusion des index Pyserini...")
        os.makedirs(index_path, exist_ok=True)
        index_dirs = " ".join(batch_paths)
        os.system(f"python -m pyserini.index.merge_indexes --index {index_path} --indexes {' '.join(batch_paths)}")

    if creer_titres_passages:
        dict_passages = cfd.construire_dict_passages(collection)
        convertir_collection_en_jsonl(dict_passages, jsonl_file)
        ids, dict_titres = cfd.construire_dict_titres_et_ids(fichier_titres)
        cfd.construit_fichier_passages(fichier_titres, fichier_titres_passages, dict_passages, dict_titres)
    else:
        ids, dict_titres, dict_passages = cfd.charger_ids_titres_passages(fichier_titres_passages)

    voisins = calculer_voisins_pyserini(dict_passages, 2, index_path, fichier_logs_premiers)

    nb_doublons_total, nb_paires_doublons, taux_doublons_total,total_paires,nb_passages = taux_global_titres_identiques(dict_titres, 3000)
    print("nb_passages : ", nb_passages)
    #print("nb paires évaluées : ", total_paires)
    print("nb_doublons_total : ", nb_doublons_total)
    print("nb_paires_doublons : ", nb_paires_doublons)
    print("taux_doublons_total : ", taux_doublons_total) #taux de titres en double sur tous les passages répondant à au moins une requête
    temps_execution1 = time.time() - start_time1
    print("temps_execution : ",temps_execution1)

    start_time2 = time.time()
    dict_passages, dict_req, dict_passages_req, dict_titres = cfd.construire_dict_passages_repondant_au_moins_1(fichier_passages_requetes, fichier_titres)
    nb_passages_au_moins_1=len(dict_passages)
    print("nb_passages_au_moins_1 : ",nb_passages_au_moins_1)
    #print("nb titres au moins 1 : ",len(dict_titres))
    dice_score_req, nb_paires_req, nb_paires_doublons_req, _ = dc.dice_moyen_requetes(dict_titres, dict_passages_req, dict_req, voisins)
    nb_passages_indexer=len(dict_passages)
    print("Nombre de passages à indexer :", nb_passages_indexer)
    #print("nb_paires_req (nombre de paires de passages répondant à au moins 1 requête) : ", nb_paires_req)
    print("nb_paires_doublons_req (paires ayant le même titre) : ", nb_paires_doublons_req)
    if dice_score_req == None :
        print("dice_score_req : ",dice_score_req," (Pas de doublons voisins pour calculer le score de dice)")
    else : 
        print("dice_score_req : ", dice_score_req)

    temps_execution2 = time.time() - start_time2
    print("temps_execution : ",temps_execution2)
    
    logs(fichier_log_similaires,nb_passages,nb_doublons_total,nb_paires_doublons,taux_doublons_total,temps_execution1,nb_passages_indexer,nb_paires_doublons_req,temps_execution2, dice_score_req, fichier_titres)

def taux_global_titres_identiques(dict_titres,nb_passages):
    total_paires = 0
    nb_doublons=0
    nb_paires_doublons=0
    nb_titres=len(dict_titres)
    for id1 in dict_titres.keys():
        for id2 in dict_titres.keys():
            total_paires += 1
            if(id1 != id2 and dict_titres.get(id1)==dict_titres.get(id2)):
                #print("id1 : ",id1)
                #print("id2 : ",id2)
                nb_doublons+=2
                nb_paires_doublons+=1
    nb_doublons=nb_doublons/2
    nb_paires_doublons=nb_paires_doublons/2
    total_paires=total_paires/2
    return nb_doublons,nb_paires_doublons,nb_doublons/nb_passages,total_paires,nb_titres #paires miroir

def logs(fichier_log,nb_passages,nb_doublons_total,nb_paires_doublons,taux_doublons_total,temps_execution1,nb_passages_indexer,nb_paires_doublons_req,temps_execution2,dice_score_req,taille_titres):
    print(f"[test {nb_passages}] Écriture du log dans {fichier_log}...")
    with open(fichier_log, "a", encoding="utf-8") as f:
        f.write(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"taille des titres : {taille_titres}\n")  
        f.write(f"Nb passages : {nb_passages}\n")
        f.write(f"nb_doublons_total : {nb_doublons_total}\n")
        f.write(f"nb_paires_doublons : {nb_paires_doublons}\n")  
        f.write(f"taux_doublons_total : {taux_doublons_total}\n")  
        f.write(f"Temps d'exécution: {temps_execution1} secondes\n")
        f.write(f"Nombre de passages à indexer : {nb_passages_indexer}\n")  
        f.write(f"Nb paires ayant le même titre : {nb_paires_doublons_req}\n")  
        f.write(f"dice_score_req : {dice_score_req}\n")  
        f.write(f"Temps d'exécution: {temps_execution2} secondes\n\n")

def logs_premiers(fichier_log,passage,premier_est_lui_meme,autre_premier):
    with open(fichier_log, "w", encoding="utf-8") as f:
            f.write(f"Passage                                      : {passage}\n")
            f.write(f"A lui-même comme plus proche voisin par BM25 : {premier_est_lui_meme}\n")
            if(not premier_est_lui_meme):
                f.write(f"Plus proche voisin                       : {autre_premier}\n\n")

def lancement_3000(creer_titres_passages,indexer):
    fichier_titres="data/titres_3000.tsv"
    fichier_log_similaires="src/logs/similaires/similaires_3000.log"
    fichier_logs_premiers="src/logs/similaires/premiers_3000.log"
    fichier_titres_passages="data/titres_passages_3000.tsv"
    nb_passages=3000
    fichier_passages_requetes="data/passages_queries.tsv" #fichier contenant le passage et la requête à laquelle il répond
    collection="data/collection.tsv"
    traitement(fichier_titres,fichier_log_similaires,fichier_logs_premiers,collection,fichier_titres_passages,fichier_passages_requetes,nb_passages,creer_titres_passages,indexer,"9-21")

def lancement_3000_petit(creer_titres_passages,indexer):
    fichier_titres="data/MS_short_titres_vllm_3000.tsv"
    fichier_log_similaires="src/logs/similaires/similaires_3000.log"
    fichier_logs_premiers="src/logs/similaires/premiers_3000.log"
    fichier_titres_passages="data/titres_passages_3000_short.tsv"
    nb_passages=3000
    fichier_passages_requetes="data/passages_queries.tsv" #fichier contenant le passage et la requête à laquelle il répond
    collection="data/collection.tsv"
    traitement(fichier_titres,fichier_log_similaires,fichier_logs_premiers,collection,fichier_titres_passages,fichier_passages_requetes,nb_passages,creer_titres_passages,indexer,"4-16")

def lancement_msmarco(creer_titres_passages,indexer):
    fichier_titres="data/titres.tsv"
    fichier_log_similaires="src/logs/similaires/similaires.tsv"
    fichier_logs_premiers="src/logs/similaires/premiers.tsv"
    fichier_titres_passages="data/titres_passages_tout.tsv"
    nb_passages="tout"
    fichier_passages_requetes="data/passages_queries.tsv" #fichier contenant le passage et la requête à laquelle il répond
    collection="data/collection.tsv"
    traitement(fichier_titres,fichier_log_similaires,fichier_logs_premiers,collection,fichier_titres_passages,fichier_passages_requetes,nb_passages,creer_titres_passages,indexer,"9-21")

def lancement_msmarco_petit(creer_titres_passages,indexer):
    fichier_titres="data/MS_short_titres_vllm.tsv"
    fichier_log_similaires="src/logs/similaires/similaires.tsv"
    fichier_logs_premiers="src/logs/similaires/premiers.tsv"
    fichier_titres_passages="data/titres_passages_tout.tsv"
    nb_passages="tout"
    fichier_passages_requetes="data/passages_queries.tsv" #fichier contenant le passage et la requête à laquelle il répond
    collection="data/collection.tsv"
    traitement(fichier_titres,fichier_log_similaires,fichier_logs_premiers,collection,fichier_titres_passages,fichier_passages_requetes,nb_passages,creer_titres_passages,indexer,"4-16")

if __name__ == "__main__":
    creer_titres_passages=False #différent pour les titres longs et courts
    indexer=False
    lancement_3000(creer_titres_passages,indexer)
    #lancement_3000_petit(creer_titres_passages,indexer)
    #lancement_msmarco(creer_titres_passages,indexer)
