from collections import defaultdict
import time
from datetime import datetime
from itertools import combinations
import creer_fichier_dict as cfd

#construit un dictionnaire id_req:[ids_passages_pertinents]
def construire_req_to_passages(qrels):
    print("construire_req_to_passages")
    req_to_passages=defaultdict(list)
    with open(qrels, 'r', encoding='utf-8') as f:
        for line in f:
            l=line.strip().split('\t')
            req_to_passages[l[0]].append(l[2])
    return req_to_passages

def parcours_req(req_to_passages,dict_titres,fichier_passages_pertinents,fichier_titres_pertinents,fichier_titres_doublons,fichier_log_pertinents):
    start_time = time.time()
    print("parcours_req")
    nb_paires_doublons=0
    nb_req=0
    nb_passages=0
    with open(fichier_passages_pertinents, 'w', encoding='utf-8') as f,open(fichier_titres_pertinents, 'w', encoding='utf-8') as f2,open(fichier_titres_doublons, 'w', encoding='utf-8') as f3,open(fichier_log_pertinents, 'a', encoding='utf-8') as f4:
        for id_r,passages in req_to_passages.items():
            if len(passages)>1:
                #écrire la requête et les passages pertinents dans le fichier
                #print("passages : ",passages)
                ids_passages_pertinents="\t".join(passages)#séparateur \t
                titres=[dict_titres.get(id_p) for id_p in passages]
                #print("titres : ",titres)
                titres_passages_pertinents="\t".join(titres)#séparateur \t
                f.write(f"{id_r}\t{ids_passages_pertinents}\n")
                f2.write(f"{id_r}\t{titres_passages_pertinents}\n")
                nb_passages+=len(passages)
                nb_req+=1
                #calcul du taux de doublons
                for id_p1, id_p2 in combinations(passages, 2):
                    titre1 = dict_titres.get(id_p1)
                    titre2 = dict_titres.get(id_p2)
                    if titre1 and titre2 and titre1 == titre2:
                        f3.write(f"{id_p1}\t{id_p2}\t{titre1}\n")
                        print("titre doublon : ", titre1)
                        nb_paires_doublons += 1
        taux_doublons=2*nb_paires_doublons/nb_passages
        temps_execution = time.time() - start_time
        f4.write(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f4.write(f"{nb_paires_doublons} paires de doublons\n")
        f4.write(f"{nb_req} requêtes avec au moins deux passages pertinents\t{nb_passages}\t{taux_doublons}\n")
        f4.write(f"{nb_passages} passages pertinents pour ces requêtes\t{taux_doublons}\n")
        f4.write(f"{taux_doublons} proportion de doublons parmi tous les passages\n")
        f4.write(f"{temps_execution} secondes\n")
    return nb_paires_doublons,nb_req,nb_passages

def test():
    #collection="data/collection.tsv"
    fichier_titres="data/titres.tsv"
    fichier_passages_pertinents="src/logs/pertinents/logs_passages_pertinents.log"
    fichier_titres_pertinents="src/logs/pertinents/logs_titres_passages_pertinents.log"
    fichier_titres_doublons="src/logs/pertinents/logs_titres_doublons.log"
    fichier_log_pertinents="src/logs/pertinents/logs_pertinents.log"
    qrels = "data/qrels.train.tsv"
    req_to_passages=construire_req_to_passages(qrels)
    _,dict_titres=cfd.construire_dict_titres_et_ids(fichier_titres)
    #dict_passages=aled.construire_dict_passages(collection)
    nb_doublons,nb_req,nb_passages=parcours_req(req_to_passages,dict_titres,fichier_passages_pertinents,fichier_titres_pertinents,fichier_titres_doublons,fichier_log_pertinents)
    print("nb_doublons : ",nb_doublons)
    print("nb_req : ",nb_req)
    print("nb_passages : ",nb_passages)

def test_short():
    fichier_passages_pertinents="src/logs/pertinents/logs_passages_pertinents_short.log"
    fichier_titres_pertinents="src/logs/pertinents/logs_titres_passages_pertinents_short.log"
    fichier_titres_doublons="src/logs/pertinents/logs_titres_doublons_short.log"
    fichier_titres="data/MS_short_titres_vllm_corrected.tsv"
    fichier_log_pertinents="src/logs/pertinents/logs_pertinents_short.log"
    qrels = "data/qrels.train.tsv"
    req_to_passages=construire_req_to_passages(qrels)
    _,dict_titres=cfd.construire_dict_titres_et_ids(fichier_titres)
    nb_doublons,nb_req,nb_passages=parcours_req(req_to_passages,dict_titres,fichier_passages_pertinents,fichier_titres_pertinents,fichier_titres_doublons,fichier_log_pertinents)
    print("nb_doublons : ",nb_doublons)
    print("nb_req : ",nb_req)
    print("nb_passages : ",nb_passages)

    _,dict_titres=cfd.construire_dict_titres_et_ids("data/MS_short_titres_vllm_3000.tsv")
    nb_doublons = calculer_taux_doublons(dict_titres)
    print("nb_doublons : ",nb_doublons)
    print("nb_doublons : ",nb_doublons/3000)

    _,dict_titres=cfd.construire_dict_titres_et_ids("data/titres_3000.tsv")
    nb_doublons = calculer_taux_doublons(dict_titres)
    print("nb_doublons : ",nb_doublons)
    print("nb_doublons : ",nb_doublons/3000)

def calculer_taux_doublons(dict_titres):
    nb_doublons=0
    for id1 in dict_titres.keys():
        for id2 in dict_titres.keys():
            if(id1 != id2 and dict_titres.get(id1)==dict_titres.get(id2)):
                print("id1 : ",id1)
                print("id2 : ",id2)
                nb_doublons+=2
    return nb_doublons/2 #paires miroir

if __name__ == "__main__":
    test()
    #test_short()
