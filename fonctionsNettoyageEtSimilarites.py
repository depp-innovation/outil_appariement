# -*- coding: utf-8 -*-
import jellyfish as jf
import math as m


def calculerSilimariteJWNomsPrenoms(listeNoms_aa, listeNoms_bb):

    listeNoms_a = list(filter(None, listeNoms_aa))
    listeNoms_b = list(filter(None, listeNoms_bb))
    if len(listeNoms_a) == 0 or len(listeNoms_b) == 0:
        return 0
    premier_nom_a = listeNoms_a[0]
    premier_nom_b = listeNoms_b[0]
    similariteJW = jf.jaro_winkler(premier_nom_a, premier_nom_b)
    taille_a = len(listeNoms_a)
    taille_b = len(listeNoms_b)
    tailleMin = min(taille_a, taille_b)
    # cas 1 : un seul nom/prénom des deux côtés
    if taille_a == taille_b == 1:
        return similariteJW

    # cas 2 : un seul nom/prénom d'un côté et plusieurs de l'autre
    if taille_a == 1:
        similariteJWcroisee1 = jf.jaro_winkler(premier_nom_a, listeNoms_b[1])
        if taille_b > 2:
            similariteJWcroisee2 = jf.jaro_winkler(premier_nom_a, listeNoms_b[2])
        else:
            similariteJWcroisee2 = 0
        similariteJWmax = max(similariteJWcroisee1, similariteJWcroisee2)
        if similariteJWmax > similariteJW + 0.15:
            return similariteJWmax - 0.15
    if taille_b == 1:
        similariteJWcroisee1 = jf.jaro_winkler(listeNoms_a[1], premier_nom_b)
        if taille_a > 2:
            similariteJWcroisee2 = jf.jaro_winkler(listeNoms_a[2], premier_nom_b)
        else:
            similariteJWcroisee2 = 0
        similariteJWmax = max(similariteJWcroisee1, similariteJWcroisee2)
        if similariteJWmax > similariteJW + 0.15:
            return similariteJWmax - 0.15

    # cas 3 : au moins 2 noms/prénoms de chaque côté
    if similariteJW < 0.85:
        if tailleMin < 2:
            return similariteJW
        # prise en compte inversion noms/prénoms 1 et 2
        else:
            second_nom_a = listeNoms_a[1]
            second_nom_b = listeNoms_b[1]
            JWcroise1 = jf.jaro_winkler(premier_nom_a, second_nom_b)
            JWcroise2 = jf.jaro_winkler(second_nom_a, premier_nom_b)
            if JWcroise1 > 0.85 and JWcroise2 > 0.85:
                return 0.95
            else:
                return similariteJW
    else:
        # tant qu'il a une bonne concordance dans les noms/prénoms suivants on augmente la similarité
        i = 1
        while i < tailleMin:
            nom_suivant_a = listeNoms_a[i]
            nom_suivant_b = listeNoms_b[i]
            similariteJWnom_suivant = jf.jaro_winkler(nom_suivant_a, nom_suivant_b)
            if similariteJWnom_suivant > 0.85:
                similariteJW = similariteJW + 0.4
            else:
                similariteJW = similariteJW - 0.15
                return similariteJW
            i = i + 1
        return similariteJW


# ---------------------------------------------------------------------------------------------------
def calculerSilimariteLibelleCommuneNaissance(
    LibelleCommuneNaissance_a, LibelleCommuneNaissance_b
):
    if LibelleCommuneNaissance_a is None or LibelleCommuneNaissance_b is None:
        return 0
    else:
        if LibelleCommuneNaissance_a[0:5] == LibelleCommuneNaissance_b[0:5] == "PARIS":
            return 1
        if LibelleCommuneNaissance_a[0:4] == LibelleCommuneNaissance_b[0:4] == "LYON":
            return 1
        if (
            LibelleCommuneNaissance_a[0:9]
            == LibelleCommuneNaissance_b[0:9]
            == "MARSEILLE"
        ):
            return 1
        similariteLevenshtein = 1 - jf.levenshtein_distance(
            LibelleCommuneNaissance_a, LibelleCommuneNaissance_b
        ) / max(len(LibelleCommuneNaissance_a), len(LibelleCommuneNaissance_b))
        return similariteLevenshtein


# prend en compte COG et libellés même avec valeurs manquantes
def calculerSilimariteCodeCommuneNaissance(
    CodeCommuneNaissance_a,
    LibelleCommuneNaissance_a,
    CodeCommuneNaissance_b,
    LibelleCommuneNaissance_b,
):
    # 1) si COG non nuls
    if CodeCommuneNaissance_a is not None and CodeCommuneNaissance_b is not None:
        if CodeCommuneNaissance_a == CodeCommuneNaissance_b:
            # COG identiques et français
            if CodeCommuneNaissance_a[0:2] != "99":
                return 1
            # COG pays identiques mais commune potentiellement différente
            else:
                return 0.9
        else:
            # si COG différents mais appartenant à Paris, Marseille ou Lyon => 0.9
            listCOGparis = [
                "75056",
                "75101",
                "75102",
                "75103",
                "75104",
                "75105",
                "75106",
                "75107",
                "75108",
                "75109",
                "75110",
                "75111",
                "75112",
                "75113",
                "75114",
                "75115",
                "75116",
                "75117",
                "75118",
                "75119",
                "75120",
            ]
            listCOGmarseille = [
                "13055",
                "13201",
                "13202",
                "13203",
                "13204",
                "13205",
                "13206",
                "13207",
                "13208",
                "13209",
                "13210",
                "13211",
                "13212",
                "13213",
                "13214",
                "13215",
                "13216",
            ]
            listCOGlyon = [
                "69123",
                "69381",
                "69382",
                "69383",
                "69384",
                "69385",
                "69386",
                "69387",
                "69388",
                "69389",
            ]
            if (
                CodeCommuneNaissance_a in listCOGparis
                and CodeCommuneNaissance_b in listCOGparis
            ):
                return 0.9
            elif (
                CodeCommuneNaissance_a in listCOGmarseille
                and CodeCommuneNaissance_b in listCOGmarseille
            ):
                return 0.9
            elif (
                CodeCommuneNaissance_a in listCOGlyon
                and CodeCommuneNaissance_b in listCOGlyon
            ):
                return 0.9
            else:
                # similarité au niveau du département
                simDep = 0
                if CodeCommuneNaissance_a[0:2] == CodeCommuneNaissance_b[0:2]:
                    simDep = 1

                # si libellés nuls, comparaison sur le département uniquement
                if (
                    LibelleCommuneNaissance_a is None
                    or LibelleCommuneNaissance_b is None
                ):
                    return 0.5
                # si libellés non nuls et similarité JW élevée => soit commune homonyme, soit erreur dans le COG
                else:
                    simLib = calculerSilimariteLibelleCommuneNaissance(
                        LibelleCommuneNaissance_a, LibelleCommuneNaissance_b
                    )
                    # quelques communes homonymes au sein des departements DOM (97) = 40
                    # aucune commune homonymes au sein des departements DOM (3 chiffres)
                    # aucune commune homonymes au sein d'un même département FM
                    # erreur dans le COG probable => on donne un bonus par rapport à simDep et on se rapproche d'une similarité uniquement sur les noms
                    if simDep == 1 and simLib >= 0.8:
                        return 0.7
                    # communes différentes mais département identique => on donne 0.5
                    elif simDep == 1:
                        return 0.5
                    # plus de 3000 communes homonymes entre plusieurs départements
                    # erreur dans le COG peu probable => on donne un bonus par rapport à simDep et on se rapproche d'une similarité uniquement sur les noms
                    elif simDep == 0 and simLib >= 0.8:
                        return 0.3
                    else:
                        return 0

    # 2) si COG nuls et libellés non nuls => similarité sur les libellés uniquement
    elif (
        LibelleCommuneNaissance_a is not None and LibelleCommuneNaissance_b is not None
    ):
        simLib = calculerSilimariteLibelleCommuneNaissance(
            LibelleCommuneNaissance_a, LibelleCommuneNaissance_b
        )
        return simLib

    # 3) si COG nuls et libellés nuls => similarite=0.2
    else:
        return 0.2


# -------------------------------------------------------------------------------------------------
def convert_to_int(val):
    try:
        return int(val)
    except Exception:
        return None


# -------------------------------------------------------------------------------------------------
def simDatesNaissance(
    date_a_jour, date_a_mois, date_a_annee, date_b_jour, date_b_mois, date_b_annee
):
    date_a_jour = convert_to_int(date_a_jour)
    date_a_mois = convert_to_int(date_a_mois)
    date_a_annee = convert_to_int(date_a_annee)
    date_b_jour = convert_to_int(date_b_jour)
    date_b_mois = convert_to_int(date_b_mois)
    date_b_annee = convert_to_int(date_b_annee)

    if (
        date_a_jour is None
        or date_a_mois is None
        or date_a_annee is None
        or date_b_jour is None
        or date_b_mois is None
        or date_b_annee is None
    ):
        if (
            date_a_annee is not None
            and date_a_annee == date_b_annee
            and date_a_mois is not None
            and date_a_mois == date_b_mois
        ):
            return 0.8
        if (
            date_a_annee is not None
            and date_a_annee == date_b_annee
            and date_a_jour is not None
            and date_a_jour == date_b_jour
        ):
            return 0.6
        if (
            date_a_mois is not None
            and date_a_mois == date_b_mois
            and date_a_jour is not None
            and date_a_jour == date_b_jour
        ):
            return 0.2

    else:  # tous les champs sont renseignés (pas de valeurs manquantes)
        date_a = str(date_a_jour) + str(date_a_mois) + str(date_a_annee)
        date_b = str(date_b_jour) + str(date_b_mois) + str(date_b_annee)
        if date_a == date_b:
            return 1
        else:
            distanceLevenshteinDates = jf.levenshtein_distance(date_a, date_b)
            # erreur uniquement sur le jour
            if date_a_annee == date_b_annee and date_a_mois == date_b_mois:
                if distanceLevenshteinDates < 2:
                    return 0.9
                return 0.8
            # année identique, inversion jour et mois
            if (
                date_a_annee == date_b_annee
                and date_a_mois == date_b_jour
                and date_a_jour == date_b_mois
            ):
                return 0.65
            if (
                date_a_annee == date_b_annee
                and date_b_mois == date_a_jour
                and date_b_jour == date_a_mois
            ):
                return 0.65
            # erreur uniquement sur le mois
            if date_a_annee == date_b_annee and date_a_jour == date_b_jour:
                if distanceLevenshteinDates < 2:
                    return 0.7
                return 0.6
            # erreur uniquement sur la dizaine de l'année
            if date_a[0:6] == date_b[0:6] and date_a[7:8] == date_b[7:8]:
                return 0.4
            # erreur uniquement sur l'année
            if date_a_mois == date_b_mois and date_a_jour == date_b_jour:
                if distanceLevenshteinDates < 2:
                    return 0.3
                return 0.2
            # GD: année identique
            if date_a_annee == date_b_annee:
                return 0.1
    return 0


# ------------------------------------------------------------------------------------------------------------------------------
def simBinaire(var_a, var_b):
    if var_a is None or var_b is None:
        return 0
    if var_a == var_b:
        return 1
    else:
        return 0


# GD: version avec l'attribut minimumValue dans decision
# si une des similarités est < à un min donné => on met decision=KO
# voir si meilleur code possible (ex: ne pas faire de boucle si aucune balise minimumValue est renseignée?)
def calculScoreConfiance(listeValuesVariables, listeSimilarityUsage):
    score, i = 0, -1
    for value in listeValuesVariables:
        i += 1
        similarityUsage = listeSimilarityUsage[i]
        # print("value=> ",value," similarityUsage=> ",similarityUsage)
        wheight = float(similarityUsage.wheight)
        add = float(similarityUsage.add)
        power = float(similarityUsage.power)
        score += wheight * m.pow(value + add, power)
    return score


def decision(
    score_confiance, seuil, dicoModalites, listeValuesVariables, listeSimilarityUsage
):
    score0, i = 0, -1
    for value in listeValuesVariables:
        i += 1
        similarityUsage = listeSimilarityUsage[i]
        # print("value=> ",value," similarityUsage=> ",similarityUsage)
        if similarityUsage.minimumValue is not None:
            minimumValue = float(similarityUsage.minimumValue)
            if value < minimumValue:
                score0 += 1

    if score_confiance >= float(seuil) and score0 == 0:
        return dicoModalites["positive"]
    else:
        return dicoModalites["negative"]