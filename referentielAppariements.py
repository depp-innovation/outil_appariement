# -*- coding: utf-8 -*-
import pickle
import os
import objectsRL as oRL
import logging


logger = logging.getLogger("prototypeRecordLinkage")


def updateSpecificationExecutionParametersReferentielDebut(
    cheminSpecificationXML
):
    try:
        pickle_in_dictionnaireSpecificationExecutionParameters = open(
            "dictionnaireSpecificationExecutionParameters.pickle", "rb"
        )
        dictionnaireSpecificationExecutionParameters = pickle.load(
            pickle_in_dictionnaireSpecificationExecutionParameters
        )
        pickle_in_dictionnaireSpecificationExecutionParameters.close()
    except Exception as e:

        logger.info(
            "première spécification (donc forcément nouvelle) dans ce contexte Python"
        )
        dictionnaireSpecificationExecutionParameters = {}
        dateDerniereModificationXML, isDerniereExecutionOK, executeAll = (
            os.path.getmtime(cheminSpecificationXML),
            False,
            True,
        )
        specificationExecutionParameters = oRL.SpecificationExecutionParameters(
            cheminSpecificationXML,
            dateDerniereModificationXML,
            isDerniereExecutionOK,
            executeAll,
        )
        dictionnaireSpecificationExecutionParameters[
            cheminSpecificationXML
        ] = specificationExecutionParameters
        pickle_out = open("dictionnaireSpecificationExecutionParameters.pickle", "wb")
        pickle.dump(dictionnaireSpecificationExecutionParameters, pickle_out)
        pickle_out.close()
        return specificationExecutionParameters

    # la spécification a déjà été lancée au moins une fois
    if cheminSpecificationXML in dictionnaireSpecificationExecutionParameters:
        specificationExecutionParameters = dictionnaireSpecificationExecutionParameters[
            cheminSpecificationXML
        ]
        dateDerniereModificationConnue = (
            specificationExecutionParameters.dateDerniereModificationXML
        )
        dateDerniereModificationFichier = os.path.getmtime(cheminSpecificationXML)

        # ET le XML n'a pas été modifié depuis le dernier lancement
        if dateDerniereModificationConnue == dateDerniereModificationFichier:
            logger.info(
                f"la specification {os.path.basename(cheminSpecificationXML)} a déjà été exécutée et le fichier XML n'a pas été modifié depuis"
            )
            if specificationExecutionParameters.isDerniereExecutionOK:
                specificationExecutionParameters.executeAll = False
            else:
                specificationExecutionParameters.executeAll = True
            dictionnaireSpecificationExecutionParameters[
                cheminSpecificationXML
            ] = specificationExecutionParameters
            pickle_out = open(
                "dictionnaireSpecificationExecutionParameters.pickle", "wb"
            )
            pickle.dump(dictionnaireSpecificationExecutionParameters, pickle_out)
            pickle_out.close()
            return specificationExecutionParameters

        else:
            # le XML a été modifié : il faut donc tout exécuter et la dernière exécution n'est pas OK sur cette nouvelle version du XML
            logger.info(
                f"la specification {os.path.basename(cheminSpecificationXML)} a déjà été exécutée et le fichier XML a  été modifié depuis"
            )
            specificationExecutionParameters.isDerniereExecutionOK, specificationExecutionParameters.executeAll = (
                False,
                True,
            )
            specificationExecutionParameters.dateDerniereModificationXML = (
                dateDerniereModificationFichier
            )
            dictionnaireSpecificationExecutionParameters[
                cheminSpecificationXML
            ] = specificationExecutionParameters
            pickle_out = open(
                "dictionnaireSpecificationExecutionParameters.pickle", "wb"
            )
            pickle.dump(dictionnaireSpecificationExecutionParameters, pickle_out)
            pickle_out.close()
            return specificationExecutionParameters
    else:
        # la spécification est nouvelle
        logger.info(
            f"la specification {os.path.basename(cheminSpecificationXML)} est nouvelle"
        )
        dateDerniereModificationXML, isDerniereExecutionOK, executeAll = (
            os.path.getmtime(cheminSpecificationXML),
            False,
            True,
        )
        specificationExecutionParameters = oRL.SpecificationExecutionParameters(
            cheminSpecificationXML,
            dateDerniereModificationXML,
            isDerniereExecutionOK,
            executeAll,
        )
        dictionnaireSpecificationExecutionParameters[
            cheminSpecificationXML
        ] = specificationExecutionParameters
        pickle_out = open("dictionnaireSpecificationExecutionParameters.pickle", "wb")
        pickle.dump(dictionnaireSpecificationExecutionParameters, pickle_out)
        pickle_out.close()
        return specificationExecutionParameters


def updateSpecificationExecutionParametersReferentielFin(
    cheminSpecificationXML
):
    try:
        pickle_in_dictionnaireSpecificationExecutionParameters = open(
            "dictionnaireSpecificationExecutionParameters.pickle", "rb"
        )
        dictionnaireSpecificationExecutionParameters = pickle.load(
            pickle_in_dictionnaireSpecificationExecutionParameters
        )
        pickle_in_dictionnaireSpecificationExecutionParameters.close()
    except Exception as e:
        logger.error(
            "on devrait retrouver le dictionnaire pickle et réussir à le charger"
        )
        return

    if cheminSpecificationXML in dictionnaireSpecificationExecutionParameters:
        specificationExecutionParameters = dictionnaireSpecificationExecutionParameters[
            cheminSpecificationXML
        ]
        dateDerniereModificationConnue = (
            specificationExecutionParameters.dateDerniereModificationXML
        )
        dateDerniereModificationFichier = os.path.getmtime(cheminSpecificationXML)

        if dateDerniereModificationConnue == dateDerniereModificationFichier:
            specificationExecutionParameters.isDerniereExecutionOK, specificationExecutionParameters.executeAll = (
                True,
                False,
            )
            dictionnaireSpecificationExecutionParameters[
                cheminSpecificationXML
            ] = specificationExecutionParameters
            pickle_out = open(
                "dictionnaireSpecificationExecutionParameters.pickle", "wb"
            )
            pickle.dump(dictionnaireSpecificationExecutionParameters, pickle_out)
            pickle_out.close()
            return
        else:
            specificationExecutionParameters.dateDerniereModificationXML = (
                dateDerniereModificationFichier
            )
            dictionnaireSpecificationExecutionParameters[
                cheminSpecificationXML
            ] = specificationExecutionParameters
            pickle_out = open(
                "dictionnaireSpecificationExecutionParameters.pickle", "wb"
            )
            pickle.dump(dictionnaireSpecificationExecutionParameters, pickle_out)
            pickle_out.close()
            logger.error(
                f"la spécification {os.path.basename(cheminSpecificationXML)} a été modifiée pendant l'exécution du batch"
            )
            return

    else:
        logger.error(
            f"la spécification {os.path.basename(cheminSpecificationXML)} devrait être dans le dictionnaire dictionnaireSpecificationExecutionParameters"
        )
        return
