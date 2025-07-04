def dice_moyen_requetes(dict_titres,dict_passages_req,dict_req,voisins):
    print("calcul score de dice")
    #test
    #print("3694032" in dict_passages_req)
    #print("1096964" in dict_passages_req)

    scores=[]
    nb_doublons=0
    nb_paires_doublons=0
    nb_paires = len(voisins)
    id_titres_doublons="" #pour les logs
    for id_p1,id_p2 in voisins:
        #print("id_p1 : ",id_p1)
        #print("id_p2 : ",id_p2)
        t1=str(dict_titres.get(id_p1))
        t2=str(dict_titres.get(id_p2))
        if t1 == t2:
            id_titres_doublons=id_p1+" ; "+id_p2+"\n"+t1+ " ; "+ t2+"\n"
            nb_doublons+=2
            nb_paires_doublons+=1
            print("titres voisins égaux : ",id_p1,id_p2,t1,t2)
            
            #vérifier que les id de passages aient une requête associée dans le dictionnaire
            #if id_p1 in dict_passages_req and id_p2 in dict_passages_req:
            print("les voisins répondent un passage et sont ajoutés au dictionnaire")
            ens_r1 = dict_passages_req[id_p1]
            ens_r2 = dict_passages_req[id_p2]
            print("ens_r1 : ",ens_r1)
            print("ens_r2 : ",ens_r2)
            scores.append(dice_score_ensemble(ens_r1, ens_r2, dict_req))
            #else:
            #    print(f"passage manquant : {id_p1 if id_p1 not in dict_passages_req else ''} {id_p2 if id_p2 not in dict_passages_req else ''}")
            
    print("scores : ",scores)
    score_moyen=None
    print("nb_doublons : ",nb_doublons)
    if (nb_doublons!=0 and scores!=[None]):
        score_moyen = (sum(scores) / nb_paires)
    return score_moyen,nb_paires, nb_paires_doublons, id_titres_doublons

#calcul duscore de Dice entre deux ensembles d'ID de requêtes (chaînes de caractères)
#e1,e2 ensembles d'id de requêtes
#dict_req : dictionnaire id_req:req_texte
def dice_score_ensemble(e1, e2, dict_req):
    print("calcul score de dice")
    #cnvertir les ids de requêtes en chaînes de requêtes
    if(e1):
        r1 = set(dict_req.get(rid) for rid in e1)
    if(e2):
        r2 = set(dict_req.get(rid) for rid in e2)
    else : 
        print("impossible de calculer le score de dice, pas de doublons entre passages voisins")
        return None
    #calcul du score
    intersection_size = len(r1.intersection(r2))
    dice = 2 * intersection_size / (len(r1) + len(r2))
    return dice

def nouv_dice_score_moyen(dict_titres,dict_passages_req,dict_req,voisins):
    scores=[]
    nb_doublons=0
    nb_paires_doublons=0
    nb_paires = len(voisins)
    id_titres_doublons="" #pour les logs
    for id_p1,id_p2 in voisins:
        t1=dict_titres.get(id_p1)
        t2=dict_titres.get(id_p2)
        if t1 == t2:
            id_titres_doublons=id_p1+" ; "+id_p2+"\n"+t1+ " ; "+ t2+"\n"
            nb_doublons+=2
            nb_paires_doublons+=1
            print("titres égaux : ",id_p1,id_p2,t1,t2)
            ens_r1=dict_passages_req.get(id_p1)
            ens_r2=dict_passages_req.get(id_p2)
            scores.append(dice_score_ensemble(ens_r1,ens_r2,dict_req))
    print("scores : ",scores)
    score_moyen=None
    if (nb_doublons!=0):
        score_moyen = (sum(scores) / nb_paires)
    return score_moyen,nb_paires, nb_paires_doublons, id_titres_doublons
