# -*- coding: utf-8 -*-
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.svm import SVC

import fonctionsNettoyageEtSimilarites as fns

# logger = logging.getLogger("prototypeRecordLinkage")


def loadPairesFromBDDtoDF(tableName, connection, logger, schema="public"):

    sql = f"""
            SELECT count(*) AS nb
            FROM information_schema.tables
            WHERE lower(table_schema) = lower('{schema}')
            AND lower(table_name) = lower('{tableName}');
    """
    result = connection.execute(sql)
    nb = result.fetchone()["nb"]
    if nb > 0:
        paires = pd.read_sql(f"SELECT * FROM {schema}.{tableName};", connection)
        logger.info(
            f"loadPairesDataFramePandas: {len(paires)} paires chargées en dataframe à partir de {schema}.{tableName}"
        )
        return paires
    logger.warn(f"La table {schema}.{tableName} n'existe pas")
    return None


def checkEvalDF(tableName, paires, annotationColumnName, dicoModalites, logger):
    if paires is None:
        return False
    if annotationColumnName not in list(paires.columns):
        logger.error(
            f"{tableName} n'a pas la colonne {annotationColumnName} (colonne avec les annotations)"
        )
        return False
    if not set(paires[annotationColumnName]).issubset(dicoModalites.values()):
        logger.error(
            f"La colonne {annotationColumnName} (colonne avec les annotations) de la table {tableName} a des modalités incorrectes. Toutes les modalités doivent être dans {str(dicoModalites.values())}"
        )
        return False
    return True


def loadPairesFromDFtoBDD(pairesDF, tableName, schema, connection, logger):
    try:
        sql = f"DROP table IF EXISTS {schema}.{tableName};"
        connection.execute(sql)
        pairesDF.to_sql(tableName, schema=schema, con=connection, chunksize=1000)
    except Exception as e:
        logger.error(f"loadPairesFromDFtoBDD impossible pour {tableName}")
        logger.error(str(e))


def similaritiesCalculus(paires, listSimilaritiesCalculus, logger):

    if paires is not None:
        if paires.shape[0] > 0:
            logger.info(f"étape similaritiesCalculus")
            for s in listSimilaritiesCalculus:
                similarityMethod = s.similarityMethod
                similarityName = s.similarityName
                d1 = s.listeDicoParametreNomColumn[0]
                d2 = s.listeDicoParametreNomColumn[1]
                if similarityMethod == "jaroWincklerSimilarityNames":
                    l1, l2 = [d1["name1"]], [d2["name1"]]
                    if "name2" in d1:
                        l1.append(d1["name2"])
                    if "name2" in d2:
                        l2.append(d2["name2"])
                    if "name3" in d1:
                        l1.append(d1["name3"])
                    if "name3" in d2:
                        l2.append(d2["name3"])
                    paires[similarityName] = paires.apply(
                        lambda row: fns.calculerSilimariteJWNomsPrenoms(
                            [row[e] for e in l1], [row[e] for e in l2]
                        ),
                        axis=1,
                    )

                elif similarityMethod == "levenshteinSimilarityBirthPlace":
                    paires[similarityName] = paires.apply(
                        lambda row: fns.calculerSilimariteLibelleCommuneNaissance(
                            row[d1["BirthPlaceName"]], row[d2["BirthPlaceName"]]
                        ),
                        axis=1,
                    )

                elif similarityMethod == "levenshteinSimilarityBirthPlaceCOG":
                    paires[similarityName] = paires.apply(
                        lambda row: fns.calculerSilimariteCodeCommuneNaissance(
                            row[d1["BirthPlaceCode"]],
                            row[d1["BirthPlaceName"]],
                            row[d2["BirthPlaceCode"]],
                            row[d2["BirthPlaceName"]],
                        ),
                        axis=1,
                    )

                elif similarityMethod == "similarityBirthDate":
                    paires[similarityName] = paires.apply(
                        lambda row: fns.simDatesNaissance(
                            row[d1["day"]],
                            row[d1["month"]],
                            row[d1["year"]],
                            row[d2["day"]],
                            row[d2["month"]],
                            row[d2["year"]],
                        ),
                        axis=1,
                    )

                elif similarityMethod == "binary":
                    paires[similarityName] = paires.apply(
                        lambda row: fns.simBinaire(
                            row[d1["columnName"]], row[d2["columnName"]]
                        ),
                        axis=1,
                    )

                logger.info(f"{similarityName} ({similarityMethod}) - ok")


def classificationEtEvaluation(
    pairesDF, pairesEvalDF, listClassification, classificationEvaluationTable, logger
):
    for classification in listClassification:
        if classification.methodName == "handscore":
            logger.info("étape classification handScore")
            listeNomsColonnes = classification.dicoSimilarities.keys()

            if pairesDF.shape[0] > 0:
                pairesDF["score_confiance"] = pairesDF.apply(
                    lambda row: fns.calculScoreConfiance(
                        [row[e] for e in listeNomsColonnes],
                        list(classification.dicoSimilarities.values()),
                    ),
                    axis=1,
                )

                pairesDF["prediction_hand_score"] = pairesDF.apply(
                    lambda row: fns.decision(
                        row["score_confiance"],
                        classification.dicoParameters["thresholdValue"],
                        classificationEvaluationTable.dicoModalites,
                        [row[e] for e in listeNomsColonnes],
                        list(classification.dicoSimilarities.values()),
                    ),
                    axis=1,
                )
            else:
                pairesDF["score_confiance"] = []
                pairesDF["prediction_hand_score"] = []
                pairesDF = pairesDF.astype(
                    {"score_confiance": "float64", "prediction_hand_score": "string"}
                )

            if pairesEvalDF is not None:
                if pairesEvalDF.shape[0] > 0:
                    logger.info("étape évaluation handScore")
                    annotationColumnName = (
                        classificationEvaluationTable.annotationColumnName
                    )
                    evaluation(
                        pairesEvalDF,
                        annotationColumnName,
                        "prediction_hand_score",
                        classificationEvaluationTable.dicoModalites["positive"],
                        logger,
                    )

        if classification.methodName == "randomforest":
            if pairesEvalDF is not None:
                if pairesEvalDF.shape[0] > 0:
                    logger.info("étape classification randomForest")
                    # 1 : entrainement
                    annotationColumnName = (
                        classificationEvaluationTable.annotationColumnName
                    )
                    TreeNumberValue = classification.dicoParameters["TreeNumberValue"]
                    clf = RandomForestClassifier(
                        random_state=42, n_estimators=TreeNumberValue, oob_score=True
                    )
                    listeNomsColonnes = list(classification.dicoSimilarities)
                    X_train = pairesEvalDF[listeNomsColonnes].values
                    y_train = pairesEvalDF[annotationColumnName].values
                    clf.fit(X_train, y_train)

                    # 2 : evaluation qualité du modèle sur oob error
                    logger.info("étape évaluation randomForest (out of bag error)")
                    dfOobError = pd.DataFrame(
                        clf.oob_decision_function_, columns=clf.classes_
                    )
                    dfOobError["prediction_random_forest"] = dfOobError.idxmax(axis=1)
                    pairesEvaluation = pd.concat(
                        [
                            pairesEvalDF[annotationColumnName],
                            dfOobError["prediction_random_forest"],
                        ],
                        axis=1,
                    )

                    evaluation(
                        pairesEvaluation,
                        annotationColumnName,
                        "prediction_random_forest",
                        classificationEvaluationTable.dicoModalites["positive"],
                        logger,
                    )

                    # 3 prediction
                    prediction = clf.predict(pairesDF[listeNomsColonnes].values)
                    pairesDF = pd.concat(
                        [
                            pairesDF,
                            pd.Series(prediction, name="prediction_random_forest"),
                        ],
                        axis=1,
                    )
            else:
                logger.error(
                    "pairsToAnnotateTable doesn't exist or is empty => impossible to apply method randomForest"
                )

        if classification.methodName == "svm":
            if pairesEvalDF is not None:
                if pairesEvalDF.shape[0] > 0:
                    logger.info("étape classification SVM")
                    # 1 : entrainement
                    annotationColumnName = (
                        classificationEvaluationTable.annotationColumnName
                    )
                    kernel = classification.dicoParameters["kernel"]
                    C = classification.dicoParameters["C"]
                    clf = SVC(kernel=kernel, C=C)
                    listeNomsColonnes = list(classification.dicoSimilarities)
                    X_train = pairesEvalDF[listeNomsColonnes].values
                    X_train_scaled = preprocessing.scale(X_train)
                    y_train = pairesEvalDF[annotationColumnName].values
                    clf.fit(X_train_scaled, y_train)

                    # 2 : evaluation qualité du modèle sur cross validation
                    """
                    logger.info("étape évaluation svm (cross validation error)")
                    y_pred = cross_val_predict(clf, X_train_scaled, y_train, cv=2)
                    y_pred_s=pd.Series(y_pred,name='prediction_svm')
                    pairesEvaluation=pd.concat([pairesEvalDF[annotationColumnName],y_pred_s['prediction_svm']],axis=1)
                    evaluation(pairesEvaluation,annotationColumnName,"prediction_svm",classificationEvaluationTable.dicoModalites["positive"])
                    """

                    # 3 prediction
                    X = pairesDF[listeNomsColonnes].values
                    X_scaled = preprocessing.scale(X)
                    prediction = clf.predict(X_scaled)
                    pairesDF = pd.concat(
                        [pairesDF, pd.Series(prediction, name="prediction_svm")], axis=1
                    )
    return pairesDF


def evaluation(
    dataframeEvaluation,
    annotationColumnName,
    predictionColumnName,
    modalitePositive,
    logger,
):
    matriceConfusion = pd.crosstab(
        dataframeEvaluation[annotationColumnName],
        dataframeEvaluation[predictionColumnName],
        rownames=["statutPairesVrai"],
        colnames=["statutPairesPredit"],
    )
    logger.info(f"confusionMatrix : \n{matriceConfusion}")

    accuracy = accuracy_score(
        dataframeEvaluation[annotationColumnName].values,
        dataframeEvaluation[predictionColumnName].values,
    )
    precision = precision_score(
        dataframeEvaluation[annotationColumnName].values,
        dataframeEvaluation[predictionColumnName].values,
        pos_label=modalitePositive,
    )
    recall = recall_score(
        dataframeEvaluation[annotationColumnName].values,
        dataframeEvaluation[predictionColumnName].values,
        pos_label=modalitePositive,
    )
    f1 = f1_score(
        dataframeEvaluation[annotationColumnName].values,
        dataframeEvaluation[predictionColumnName].values,
        pos_label=modalitePositive,
    )

    logger.info(f"accuracy: {accuracy}")
    logger.info(f"precision: {precision}")
    logger.info(f"recall: {recall}")
    logger.info(f"f1: {f1}")
