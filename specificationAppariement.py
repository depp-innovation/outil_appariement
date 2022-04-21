# -*- coding: utf-8 -*-
import logging
import xml.etree.ElementTree as ET
import objectsRL as oRL
import recordLinkageTool as RL
import os
from dotenv import load_dotenv

logger = logging.getLogger("prototypeRecordLinkage")


class SpecificationAppariement:
    def __init__(self, cheminSpecificationXML, cheminFichierEnvironnement):
        self.user = ""
        self.logger = logger
        self.cheminSpecificationXML = cheminSpecificationXML
        self.cheminFichierEnvironnement = cheminFichierEnvironnement
        self.setValuesFromEnv(cheminFichierEnvironnement)
        self.XMLchecks = self.getXMLchecks()
        if len(self.XMLchecks.listeXMLWarnings) > 0:
            self.logger.warning("there are warning(s) in the XML specification ")
            for warning in self.XMLchecks.listeXMLWarnings:
                self.logger.warning(warning)

        if self.XMLchecks.isSpecificationOK:
            self.root = self.getRoot()
            self.database = self.getDatabase()
            self.listInputTables = self.getInputTables()
            self.dicoOutputTableNames = self.getDicoOutputTableNames()
            self.classificationEvaluationTable = self.getClassificationEvaluationTable()
            self.logParsedSpecification = self.getLogParsedSpecification()
            self.sqlQueryExactRecordLinkage = self.getSqlQueryExactRecordLinkage()
            self.sqlQueriesPairCreation = self.getSqlQueriesPairCreation()
            self.numberPairCreation = self.getNumberPairCreation()
            self.sqlQueryDoublesInPairsElimination = (
                self.getSqlQueryDoublesInPairsElimination()
            )
            self.listSimilaritiesCalculus = self.getListSimilaritiesCalculus()
            self.listClassification = self.getListClassification()
        else:
            msg = "there are critical(s) error(s) in the XML specification : please correct them"
            self.logger.critical(msg)
            for error in self.XMLchecks.listeXMLerror:
                self.logger.critical(error)
            raise Exception(msg)

    def runSpecification(self):
        if self.XMLchecks.isSpecificationOK:
            self.logger.info("début record linkage")
            if self.logParsedSpecification:
                self.printSpecificationDetails()
            connection = self.database.engine.connect()

            # Rechargement de la function

            connection.execute(
                """
                    DROP EXTENSION IF EXISTS fuzzystrmatch;
                    CREATE EXTENSION fuzzystrmatch;

                    DROP FUNCTION IF EXISTS public.levenshtein_similarity;

                    CREATE FUNCTION public.levenshtein_similarity(IN string_a text, IN string_b text)
                        RETURNS numeric
                        LANGUAGE 'plpgsql'
                        AS $BODY$BEGIN
                            return 1- CAST(levenshtein(string_a, string_b) as NUMERIC)/CAST(greatest(length(string_a), length(string_b)) as NUMERIC);
                        END;$BODY$;

                    ALTER FUNCTION public.levenshtein_similarity(text, text)
                        OWNER TO {};
                """.format(self.user)
            )

            self.logger.info("détails tables input")
            result = connection.execute(
                f"SELECT count(*) from {self.database.schema}.{self.listInputTables[0].tableName};"
            ).scalar()
            self.logger.info(
                f"Number of rows in {self.database.schema}.{self.listInputTables[0].tableName}  = {result} rows"
            )
            result = connection.execute(
                f"SELECT count(*) from {self.database.schema}.{self.listInputTables[1].tableName};"
            ).scalar()
            self.logger.info(
                f"Number of rows in {self.database.schema}.{self.listInputTables[1].tableName}  = {result} rows"
            )

            # appariement exact
            self.logger.info("étape ExactRecordLinkage")
            connection.execute(self.sqlQueryExactRecordLinkage)

            result = connection.execute(
                f"SELECT count(id) from {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']};"
            ).scalar()
            self.logger.info(
                f"Number of pairs in {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']} = {result} pairs"
            )
            result = connection.execute(
                f"SELECT count(distinct pk0) from {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']};"
            ).scalar()
            self.logger.info(
                f"Number of distinct primary key '{self.listInputTables[0].primaryKeyName}' in {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']} = {result} pairs"
            )
            result = connection.execute(
                f"SELECT count(distinct pk1) from {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']};"
            ).scalar()
            self.logger.info(
                f"Number of distinct primary key '{self.listInputTables[1].primaryKeyName}' in {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']} = {result} pairs"
            )

            self.logger.info("étape indexation SQL")
            for query in self.sqlQueriesPairCreation:
                connection.execute(query)

            if self.numberPairCreation > 0:
                for j in range(1, self.numberPairCreation + 1):
                    result = connection.execute(
                        f"SELECT count(id) from paires_{j};"
                    ).scalar()
                    self.logger.info(
                        f"Number of pairs in temporary table paires_{j} = {result} pairs"
                    )

            # élimination doublons
            connection.execute(self.sqlQueryDoublesInPairsElimination)

            result = connection.execute(
                f"SELECT count(id) from public.paires;"
            ).scalar()
            self.logger.info(
                f"Number of pairs in final table without duplicate = {result} pairs"
            )
            SEUIL = 6_000_000
            if result > SEUIL:
                raise Exception(
                    f"Number of pairs in final table without duplicate too big !!! {result} > {SEUIL}"
                )
            pairesDF = RL.loadPairesFromBDDtoDF(
                tableName="paires", connection=connection, logger=self.logger
            )
            if pairesDF is None:
                raise Exception(f"La table paires doit être présente.")
            pairesEvalDF = RL.loadPairesFromBDDtoDF(
                self.classificationEvaluationTable.tableName,
                connection,
                self.logger,
                self.database.schema,
            )
            if not RL.checkEvalDF(
                tableName=self.classificationEvaluationTable.tableName,
                paires=pairesEvalDF,
                annotationColumnName=self.classificationEvaluationTable.annotationColumnName,
                dicoModalites=self.classificationEvaluationTable.dicoModalites,
                logger=self.logger,
            ):
                pairesEvalDF = None

            RL.similaritiesCalculus(
                pairesDF, self.listSimilaritiesCalculus, self.logger
            )
            RL.similaritiesCalculus(
                pairesEvalDF, self.listSimilaritiesCalculus, self.logger
            )
            pairesDF = RL.classificationEtEvaluation(
                pairesDF,
                pairesEvalDF,
                self.listClassification,
                self.classificationEvaluationTable,
                self.logger,
            )

            self.logger.info("load Paires From DF to BDD")
            RL.loadPairesFromDFtoBDD(
                pairesDF,
                self.dicoOutputTableNames["fuzzyRecordLinkageTableName"],
                self.database.schema,
                connection,
                self.logger,
            )

            # NC : Ajout des colonnes pour les annotations de paires
            connection.execute(
                f"""
                    ALTER TABLE {self.database.schema}.{self.dicoOutputTableNames['fuzzyRecordLinkageTableName']}
                    ADD COLUMN prediction_annotation text
                    , ADD COLUMN poids float8;
                """
            )

            result = connection.execute(
                f"""
                    SELECT count(*)
                    FROM {self.database.schema}.{self.dicoOutputTableNames['fuzzyRecordLinkageTableName']}
                    WHERE prediction_hand_score='OK';
                """
            ).scalar()
            self.logger.info(
                f"Number of pairs OK in {self.database.schema}.{self.dicoOutputTableNames['fuzzyRecordLinkageTableName']} = {result} pairs"
            )
            result = connection.execute(
                f"SELECT count(distinct pk0) from {self.database.schema}.{self.dicoOutputTableNames['fuzzyRecordLinkageTableName']} where prediction_hand_score='OK';"
            ).scalar()
            self.logger.info(
                f"Number of distinct primary key '{self.listInputTables[0].primaryKeyName}' OK in {self.database.schema}.{self.dicoOutputTableNames['fuzzyRecordLinkageTableName']} = {result} pairs"
            )

            connection.execute(f"DROP TABLE IF EXISTS public.paires CASCADE;")
            connection.close()

            self.logger.info("fin record linkage")

    def getXMLchecks(self):
        listeXMLerror, listeXMLWarnings = [], []
        try:
            with open(self.cheminSpecificationXML):
                self.logger.info(
                    f"the XML file {self.cheminSpecificationXML} exists (OK)"
                )
        except Exception as e:
            listeXMLerror.append(
                f"the XML file {self.cheminSpecificationXML} doesn't exist" + str(e)
            )
            return oRL.XMLchecks(False, listeXMLerror, listeXMLWarnings)

        # 0 global XML parsing verification
        try:
            tree = ET.parse(self.cheminSpecificationXML)
            root = tree.getroot()
        except Exception as e:
            listeXMLerror.append(
                "the XML can't be parsed : there are XML errors in the file : " + str(e)
            )
            return oRL.XMLchecks(False, listeXMLerror, listeXMLWarnings)

        # 1 root element verification
        if root.tag != "specificationRecordlinkage":
            listeXMLerror.append(
                "in the XML, root node must be called specificationRecordlinkage"
            )

        # 2 databaseParameter element verification
        databaseParameter = root.find("databaseParameter")
        if databaseParameter is None:
            listeXMLerror.append(
                "in the XML, having a databaseParameter element (children of specificationRecordlinkage) is compulsory"
            )
        else:
            serverParameter = databaseParameter.find("serverParameter")
            if serverParameter is None:
                listeXMLerror.append(
                    "in the XML, having a serverParameter element (children of databaseParameter) is compulsory"
                )
            else:
                if serverParameter.get("server") is None:
                    listeXMLerror.append(
                        "in the XML, having a server attribute in the element serverParameter is compulsory"
                    )
                if serverParameter.get("port") is None:
                    listeXMLerror.append(
                        "in the XML, having a port attribute in the element serverParameter is compulsory"
                    )
            userParameter = databaseParameter.find("userParameter")
            if userParameter is None:
                listeXMLerror.append(
                    "in the XML, having a userParameter element (children of databaseParameter) is compulsory"
                )
            else:
                if userParameter.get("user") is None:
                    listeXMLerror.append(
                        "in the XML, having a user attribute in the element userParameter is compulsory"
                    )
                if userParameter.get("password") is None:
                    listeXMLerror.append(
                        "in the XML, having a password attribute in the element userParameter is compulsory"
                    )
            baseParameter = databaseParameter.find("baseParameter")
            if baseParameter is None:
                listeXMLerror.append(
                    "in the XML, having a baseParameter element (children of databaseParameter) is compulsory"
                )
            else:
                if baseParameter.get("database") is None:
                    listeXMLerror.append(
                        "in the XML, having a database attribute in the element baseParameter is compulsory"
                    )
                if baseParameter.get("schema") is None:
                    listeXMLerror.append(
                        "in the XML, having a schema attribute in the element baseParameter is compulsory"
                    )

        # 3 inputTable element verification
        nb = len(list(root.findall("inputTable")))
        if nb != 2:
            listeXMLerror.append(
                f"in the XML, having 2 attributes inputTable (children of specificationRecordlinkage) is compulsory : {nb} found"
            )
        if nb == 2:
            dicoTables = {}
            for table in root.findall("inputTable"):
                tableName = table.get("tableName")
                if tableName is None:
                    listeXMLerror.append(
                        "in the XML, having a tableName attribute in each element inputTable is compulsory"
                    )
                if table.get("primaryKeyName") is None:
                    listeXMLerror.append(
                        f"in the XML, having a primaryKeyName attribute in each element inputTable is compulsory (error in {tableName})"
                    )
                nb = 0
                nb = len(list(table.findall("column")))
                if nb == 0:
                    listeXMLerror.append(
                        f"in the XML, having at least one column element in children of inputTable: {tableName}"
                    )
                if nb == 1:
                    listeXMLWarnings.append(
                        f"in the XML, there is only one column element  in children of inputTable: {tableName}"
                    )
                if nb > 0:
                    for column in table.findall("column"):
                        columnName = column.get("name")
                        if columnName is None:
                            listeXMLerror.append(
                                f"in the XML, having a name attribute in each element column (children of tableName:{tableName}) is compulsory"
                            )
                        else:
                            if tableName in dicoTables:
                                dicoTables[tableName].append(columnName)
                            else:
                                dicoTables[tableName] = [columnName]

        # 4 outputTables element verification
        outputTable = root.find("outputTables")
        if outputTable is None:
            listeXMLerror.append(
                "in the XML, having a outputTables element (children of specificationRecordlinkage) is compulsory"
            )
        else:
            if outputTable.get("exactRecordLinkageTableName") is None:
                listeXMLerror.append(
                    "in the XML, having a exactRecordLinkageTableName attribute in the element outputTables is compulsory"
                )
            if outputTable.get("fuzzyRecordLinkageTableName") is None:
                listeXMLerror.append(
                    "in the XML, having a fuzzyRecordLinkageTableName attribute in the element outputTables is compulsory"
                )

        # 5 exactRecordLinkage element verification
        exactRecordLinkage = root.find("exactRecordLinkage")
        if exactRecordLinkage is None:
            listeXMLerror.append(
                "in the XML, having a exactRecordLinkage element (children of specificationRecordlinkage) is compulsory"
            )
        else:
            nb = 0
            nb = len(list(exactRecordLinkage.findall("condition")))
            if nb == 0:
                listeXMLerror.append(
                    "in the XML, having at least one condition element (children of exactRecordLinkage) is compulsory"
                )
            if nb == 1:
                listeXMLWarnings.append(
                    f"in the XML, there is only one condition specified in exactRecordLinkage"
                )
            if nb > 0:
                for condition in exactRecordLinkage.findall("condition"):
                    nb = 0
                    nb = len(list(condition.findall("filter")))
                    if nb != 2:
                        listeXMLerror.append(
                            "in the XML, having exactly 2 element filter (children of condition) is compulsory"
                        )
                    if nb > 0:
                        for filtre in condition.findall("filter"):
                            tableName = filtre.get("tableName")
                            columnName = filtre.get("columnName")
                            if tableName is None:
                                listeXMLerror.append(
                                    "in the XML, having a tableName attribute in the element filter is compulsory"
                                )
                            if columnName is None:
                                listeXMLerror.append(
                                    "in the XML, having a columnName attribute in the element filter is compulsory"
                                )
                            if tableName not in dicoTables:
                                listeXMLerror.append(
                                    f"in the XML, tableName ({tableName}) in filter element in indexing not referenced in inputTable"
                                )
                            elif columnName not in dicoTables[tableName]:
                                listeXMLerror.append(
                                    f"in the XML, columnName ({columnName}) in filter element in indexing not referenced in inputTable"
                                )
                        operator = condition.find("operator")
                        if operator is None:
                            listeXMLerror.append(
                                "in the XML, having a operator element  (children of condition) is compulsory"
                            )
                        operatorType = operator.get("type")
                        if operatorType is None:
                            listeXMLerror.append(
                                "in the XML, having a type attribute of operator element is compulsory"
                            )
                        else:
                            if operatorType not in (
                                "equals",
                                "levenshteinDistance",
                                "levenshteinSimilarity",
                            ):
                                listeXMLerror.append(
                                    "in the XML, attribute type of element operator must be equals, levenshteinDistance or levenshteinSimilarity"
                                )
                            if operatorType == "levenshteinDistance":
                                listeXMLWarnings.append(
                                    "in the XML, there is a levenshteinDistance filter in the exactRecordLinkage conditions"
                                )
                                if (
                                    operatorType == "levenshteinDistance"
                                    and operator.get("maximumDistance") is None
                                ):
                                    listeXMLerror.append(
                                        "in the XML, attribute maximumDistance of element operator is compulsory when levenshteinDistance operator is used"
                                    )
                            if operatorType == "levenshteinSimilarity":
                                listeXMLWarnings.append(
                                    "in the XML, there is a levenshteinSimilarity filter in the exactRecordLinkage conditions"
                                )
                                if (
                                    operatorType == "levenshteinSimilarity"
                                    and operator.get("minimumSimilarity") is None
                                ):
                                    listeXMLerror.append(
                                        "in the XML, attribute minimumSimilarity of element operator is compulsory when levenshteinSimilarity operator is used"
                                    )

        # 6 indexing element verification
        fuzzyRecordLinkage = root.find("fuzzyRecordLinkage")
        if fuzzyRecordLinkage is None:
            listeXMLerror.append(
                "in the XML, having a fuzzyRecordLinkage element (children of specificationRecordlinkage) is compulsory"
            )
        else:
            indexing = fuzzyRecordLinkage.find("indexing")
            if indexing is None:
                listeXMLerror.append(
                    "in the XML, having a indexing element (children of fuzzyRecordLinkage) is compulsory"
                )
            else:
                nb = 0
                nb = len(list(indexing.findall("pairCreation")))
                if nb == 0:
                    listeXMLerror.append(
                        "in the XML, having at least one pairCreation element (children of indexing) is compulsory"
                    )
                if nb == 1:
                    listeXMLWarnings.append(
                        "in the XML, having only one pairCreation element is generally not sufficient"
                    )
                if nb > 0:
                    for pairCreation in indexing.findall("pairCreation"):
                        nb = 0
                        nb = len(list(pairCreation.findall("condition")))
                        if nb == 0:
                            listeXMLerror.append(
                                "in the XML, having at least one condition element (children of pairCreation) is compulsory"
                            )
                        if nb == 1:
                            listeXMLWarnings.append(
                                "in the XML, having only one condition element is generally not sufficient"
                            )
                        if nb > 0:
                            hasOperatorEquals = False
                            for condition in pairCreation.findall("condition"):
                                nb = 0
                                nb = len(list(condition.findall("filter")))
                                if nb != 2:
                                    listeXMLerror.append(
                                        "in the XML, having exactly 2 element filter (children of condition) is compulsory"
                                    )
                                if nb > 0:
                                    for filtre in condition.findall("filter"):
                                        tableName = filtre.get("tableName")
                                        columnName = filtre.get("columnName")
                                        if tableName is None:
                                            listeXMLerror.append(
                                                "in the XML, having a tableName attribute in the element filter is compulsory"
                                            )
                                        if columnName is None:
                                            listeXMLerror.append(
                                                "in the XML, having a columnName attribute in the element filter is compulsory"
                                            )
                                        if tableName not in dicoTables:
                                            listeXMLerror.append(
                                                f"in the XML, tableName ({tableName}) in filter element in pairCreation not referenced in inputTable"
                                            )
                                        elif columnName not in dicoTables[tableName]:
                                            listeXMLerror.append(
                                                f"in the XML, columnName ({columnName}) in filter element in pairCreation not referenced in inputTable"
                                            )
                                    operator = condition.find("operator")
                                    if operator is None:
                                        listeXMLerror.append(
                                            "in the XML, having a operator element  (children of condition) is compulsory"
                                        )
                                    operatorType = operator.get("type")
                                    if operatorType is None:
                                        listeXMLerror.append(
                                            "in the XML, having a type attribute of operator element is compulsory"
                                        )
                                    else:
                                        if operatorType not in (
                                            "equals",
                                            "levenshteinDistance",
                                            "levenshteinSimilarity",
                                        ):
                                            listeXMLerror.append(
                                                "in the XML, attribute type of element operator must be equals, levenshteinDistance or levenshteinSimilarity"
                                            )
                                        if operatorType == "equals":
                                            hasOperatorEquals = True
                                        if (
                                            operatorType == "levenshteinDistance"
                                            and operator.get("maximumDistance") is None
                                        ):
                                            listeXMLerror.append(
                                                "in the XML, attribute maximumDistance of element operator is compulsory when levenshteinDistance operator is used"
                                            )
                                        if (
                                            operatorType == "levenshteinSimilarity"
                                            and operator.get("minimumSimilarity")
                                            is None
                                        ):
                                            listeXMLerror.append(
                                                "in the XML, attribute minimumSimilarity of element operator is compulsory when levenshteinSimilarity operator is used"
                                            )

                            if not hasOperatorEquals:
                                listeXMLWarnings.append(
                                    f"in the XML, it is generally better to have at least one equals condition in each pairCreation for performance reason"
                                )

        # 7 recordPairComparison element verification
        recordPairComparison = fuzzyRecordLinkage.find("recordPairComparison")
        if recordPairComparison is None:
            listeXMLerror.append(
                "in the XML, having a recordPairComparison element (children of fuzzyRecordLinkage) is compulsory"
            )
        else:
            listesimilarityName = []
            nb = 0
            nb = len(list(recordPairComparison.findall("similarity")))
            if nb == 0:
                listeXMLerror.append(
                    "in the XML, having similarity element(s) (children of recordPairComparison) is compulsory"
                )
            if nb > 0:
                for similarity in recordPairComparison.findall("similarity"):
                    similarityName = similarity.get("similarityName")
                    listesimilarityName.append(similarityName)
                    if similarityName is None:
                        listeXMLerror.append(
                            "in the XML, having a similarityName attribute  in the element similarity is compulsory"
                        )
                    similarityMethod = similarity.get("similarityMethod")
                    if similarityMethod is None:
                        listeXMLerror.append(
                            "in the XML, having a similarityMethod attribute  in the element similarity is compulsory"
                        )
                    else:
                        if similarityMethod not in (
                            "jaroWincklerSimilarityNames",
                            "levenshteinSimilarityBirthPlace",
                            "levenshteinSimilarityBirthPlaceCOG",
                            "similarityBirthDate",
                            "binary",
                        ):
                            listeXMLerror.append(
                                "in the XML, the similarityMethod attribute must have a value in : jaroWincklerSimilarityNames,levenshteinSimilarityBirthPlace,levenshteinSimilarityBirthPlaceCOG,similarityBirthDate,binary"
                            )
                        nb = 0
                        nb = len(list(similarity.findall("input")))
                        if nb != 2:
                            listeXMLerror.append(
                                "in the XML, having 2 element input (children of similarity) is compulsory"
                            )
                        if nb > 0:
                            for inputRL in similarity.findall("input"):
                                tableName = inputRL.get("tableName")
                                if tableName is None:
                                    listeXMLerror.append(
                                        "in the XML, having a tableName attribute  in the element input is compulsory"
                                    )
                                if similarityMethod == "jaroWincklerSimilarityNames":
                                    name1 = inputRL.get("name1")
                                    if tableName not in dicoTables:
                                        listeXMLerror.append(
                                            f"in the XML, tableName ({tableName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    elif name1 not in dicoTables[tableName]:
                                        listeXMLerror.append(
                                            f"in the XML, name1 ({name1}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    if name1 is None:
                                        listeXMLerror.append(
                                            "in the XML, having a name1 attribute  in the element input is compulsory for jaroWincklerSimilarityNames method"
                                        )
                                if (
                                    similarityMethod
                                    == "levenshteinSimilarityBirthPlace"
                                ):
                                    BirthPlaceName = inputRL.get("BirthPlaceName")
                                    if tableName not in dicoTables:
                                        listeXMLerror.append(
                                            f"in the XML, tableName ({tableName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    elif BirthPlaceName not in dicoTables[tableName]:
                                        listeXMLerror.append(
                                            f"in the XML, BirthPlaceName ({BirthPlaceName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    if BirthPlaceName is None:
                                        listeXMLerror.append(
                                            "in the XML, having a BirthPlaceName attribute  in the element input is compulsory for levenshteinSimilarityBirthPlace method"
                                        )

                                if (
                                    similarityMethod
                                    == "levenshteinSimilarityBirthPlaceCOG"
                                ):
                                    BirthPlaceCode = inputRL.get("BirthPlaceCode")
                                    BirthPlaceName = inputRL.get("BirthPlaceName")
                                    if tableName not in dicoTables:
                                        listeXMLerror.append(
                                            f"in the XML, tableName ({tableName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    elif BirthPlaceCode not in dicoTables[tableName]:
                                        listeXMLerror.append(
                                            f"in the XML, BirthPlaceCode ({BirthPlaceCode}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    elif BirthPlaceName not in dicoTables[tableName]:
                                        listeXMLerror.append(
                                            f"in the XML, BirthPlaceName ({BirthPlaceName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    if BirthPlaceCode is None:
                                        listeXMLerror.append(
                                            "in the XML, having a BirthPlaceCode attribute  in the element input is compulsory for levenshteinSimilarityBirthPlaceCOG method"
                                        )
                                    elif BirthPlaceName is None:
                                        listeXMLerror.append(
                                            "in the XML, having a BirthPlaceName attribute  in the element input is compulsory for levenshteinSimilarityBirthPlaceCOG method"
                                        )

                                if similarityMethod == "similarityBirthDate":
                                    day, month, year = (
                                        inputRL.get("day"),
                                        inputRL.get("month"),
                                        inputRL.get("year"),
                                    )
                                    if day is None:
                                        listeXMLerror.append(
                                            "in the XML, having a day attribute  in the element input is compulsory for similarityBirthDate method"
                                        )
                                    if month is None:
                                        listeXMLerror.append(
                                            "in the XML, having a month attribute  in the element input is compulsory for similarityBirthDate method"
                                        )
                                    if year is None:
                                        listeXMLerror.append(
                                            "in the XML, having a year attribute  in the element input is compulsory for similarityBirthDate method"
                                        )
                                    if tableName not in dicoTables:
                                        listeXMLerror.append(
                                            f"in the XML, tableName ({tableName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    else:
                                        if day not in dicoTables[tableName]:
                                            listeXMLerror.append(
                                                f"in the XML, day ({day}) in  input element in recordPairComparison not referenced in inputTable"
                                            )
                                        if month not in dicoTables[tableName]:
                                            listeXMLerror.append(
                                                f"in the XML, month ({month}) in  input element in recordPairComparison not referenced in inputTable"
                                            )
                                        if month not in dicoTables[tableName]:
                                            listeXMLerror.append(
                                                f"in the XML, month ({month}) in  input element in recordPairComparison not referenced in inputTable"
                                            )
                                if similarityMethod == "binary":
                                    columnName = inputRL.get("columnName")
                                    if columnName is None:
                                        listeXMLerror.append(
                                            "in the XML, having a columnName attribute  in the element input is compulsory for binary method"
                                        )
                                    if tableName not in dicoTables:
                                        listeXMLerror.append(
                                            f"in the XML, tableName ({tableName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )
                                    elif columnName not in dicoTables[tableName]:
                                        listeXMLerror.append(
                                            f"in the XML, columnName ({columnName}) in  input element in recordPairComparison not referenced in inputTable"
                                        )

        # 8 classification element verification
        classification = fuzzyRecordLinkage.find("classification")
        if classification is None:
            listeXMLerror.append(
                "in the XML, having a classification element (children of fuzzyRecordLinkage) is compulsory"
            )
        else:
            methodName = classification.get("methodName")
            if methodName is None:
                listeXMLerror.append(
                    "in the XML, having a methodName attribute in the element classification is compulsory"
                )
            else:
                if methodName not in ("handScore", "randomForest"):
                    listeXMLerror.append(
                        "in the XML, the methodName attribute in the element classification must have a value in : handScore,randomForest"
                    )
                if methodName == "handScore":
                    if classification.get("thresholdValue") is None:
                        listeXMLerror.append(
                            "in the XML, having a thresholdValue attribute  in the element classification is compulsory for handScore method"
                        )
                    nb = 0
                    nb = len(list(classification.findall("similarityUsage")))
                    if nb == 0:
                        listeXMLerror.append(
                            "in the XML, having at least one similarityUsage element (children of classification) is compulsory for handScore method"
                        )
                    if nb > 0:
                        for similarityUsage in classification.findall(
                            "similarityUsage"
                        ):
                            similarityName = similarityUsage.get("similarityName")
                            if similarityName is None:
                                listeXMLerror.append(
                                    "in the XML, having a similarityName attribute in the element similarityUsage is compulsory"
                                )
                            if similarityName not in listesimilarityName:
                                listeXMLerror.append(
                                    f"in the XML, similarityName {similarityName} in element similarityUsage in classification not referenced in recordPairComparison"
                                )
                            if similarityUsage.get("wheight") is None:
                                listeXMLerror.append(
                                    "in the XML, having a wheight attribute in the element similarityUsage is compulsory"
                                )

        isSpecificationOK = len(listeXMLerror) == 0
        return oRL.XMLchecks(isSpecificationOK, listeXMLerror, listeXMLWarnings)

    def getRoot(self):
        tree = ET.parse(self.cheminSpecificationXML)
        return tree.getroot()

    def getDatabase(self):
        databaseParameter = self.root.find("databaseParameter")
        serverParameter = databaseParameter.find("serverParameter")
        server, port = serverParameter.get("server"), serverParameter.get("port")
        userParameter = databaseParameter.find("userParameter")
        user, password = userParameter.get("user"), userParameter.get("password")
        baseParameter = databaseParameter.find("baseParameter")
        database, schema = baseParameter.get("database"), baseParameter.get("schema")
        return oRL.Database(server, port, user, password, database, schema, self.logger)

    def getInputTables(self):
        listInputTables = []
        for table in self.root.findall("inputTable"):
            tableName, primaryKeyName = (
                table.get("tableName").lower(),
                table.get("primaryKeyName").lower(),
            )
            listeColummsRecordLinkage = []
            for column in table.findall("column"):
                listeColummsRecordLinkage.append(column.get("name").lower())
            listInputTables.append(
                oRL.Table(tableName, primaryKeyName, listeColummsRecordLinkage)
            )
        return listInputTables

    def getDicoOutputTableNames(self):
        dicoOutputTableNames = {}
        outputTable = self.root.find("outputTables")
        dicoOutputTableNames["exactRecordLinkageTableName"] = outputTable.get(
            "exactRecordLinkageTableName"
        ).lower()
        dicoOutputTableNames["fuzzyRecordLinkageTableName"] = outputTable.get(
            "fuzzyRecordLinkageTableName"
        ).lower()
        return dicoOutputTableNames

    def getLogParsedSpecification(self):
        verbosity = self.root.find("logParsedSpecification").get("verbosity").lower()
        return verbosity == "yes"

    def getClassificationEvaluationTable(self):
        classificationEvaluationTable = self.root.find("classificationEvaluationTable")
        tableName = classificationEvaluationTable.get("tableName")
        annotationColumnName = classificationEvaluationTable.get(
            "annotationColumnName"
        ).lower()
        dicoModalites = {}
        dicoModalites["positive"] = classificationEvaluationTable.get("positive")
        dicoModalites["negative"] = classificationEvaluationTable.get("negative")
        dicoModalites["incertain"] = classificationEvaluationTable.get("incertain")
        return oRL.classificationEvaluationTable(
            tableName, annotationColumnName, dicoModalites
        )

    def getSqlQueryExactRecordLinkage(self):
        exactRecordLinkage = self.root.find("exactRecordLinkage")
        listeConditionsSQL = []
        for condition in exactRecordLinkage.findall("condition"):
            listeFilters = [filtre for filtre in condition.findall("filter")]
            operatorType = condition.find("operator").get("type")

            if operatorType == "equals":
                c = oRL.SQLCondition(
                    listeFilters[0].get("tableName").lower(),
                    listeFilters[0].get("columnName").lower(),
                    operatorType,
                    listeFilters[1].get("tableName").lower(),
                    listeFilters[1].get("columnName").lower(),
                )
                listeConditionsSQL.append(c.toString())
            if operatorType == "levenshteinDistance":
                maximumDistance = condition.find("operator").get("maximumDistance")
                c = oRL.SQLCondition(
                    listeFilters[0].get("tableName").lower(),
                    listeFilters[0].get("columnName").lower(),
                    operatorType,
                    listeFilters[1].get("tableName").lower(),
                    listeFilters[1].get("columnName").lower(),
                    maximumDistance,
                    None,
                )
                listeConditionsSQL.append(c.toString())
            if operatorType == "levenshteinSimilarity":
                minimumSimilarity = condition.find("operator").get("minimumSimilarity")
                c = oRL.SQLCondition(
                    listeFilters[0].get("tableName").lower(),
                    listeFilters[0].get("columnName").lower(),
                    operatorType,
                    listeFilters[1].get("tableName").lower(),
                    listeFilters[1].get("columnName").lower(),
                    None,
                    minimumSimilarity,
                )
                listeConditionsSQL.append(c.toString())
        sqlQueryExactRecordLinkage = f"""
        DROP table IF EXISTS {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']};

        CREATE table {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']}
        as SELECT
        {self.listInputTables[0].primaryKeyName} as pk0,
        {self.listInputTables[1].primaryKeyName} as pk1,
        {self.listInputTables[0].primaryKeyName} || {self.listInputTables[1].primaryKeyName} as id
        FROM {self.database.schema}.{self.listInputTables[0].tableName} , {self.database.schema}.{self.listInputTables[1].tableName}
        WHERE """
        for c in listeConditionsSQL:
            sqlQueryExactRecordLinkage = sqlQueryExactRecordLinkage + c + " AND "
        sqlQueryExactRecordLinkage = sqlQueryExactRecordLinkage[
            :-4
        ]  
        sqlQueryExactRecordLinkage = (
            sqlQueryExactRecordLinkage
            + f""";
        DELETE FROM {self.database.schema}.{self.listInputTables[0].tableName}
        WHERE {self.listInputTables[0].primaryKeyName} in (select pk0 from {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']});

        DELETE FROM {self.database.schema}.{self.listInputTables[1].tableName}
        WHERE {self.listInputTables[1].primaryKeyName} in (select pk1 from {self.database.schema}.{self.dicoOutputTableNames['exactRecordLinkageTableName']});
        """
        )
        return sqlQueryExactRecordLinkage

    def getSqlQueriesPairCreation(self):
        sqlQueriesPairCreation = []
        fuzzyRecordLinkage = self.root.find("fuzzyRecordLinkage")
        indexing = fuzzyRecordLinkage.find("indexing")
        i = 0
        for pairCreation in indexing.findall("pairCreation"):
            i += 1
            listeConditionsSQL = []
            for condition in pairCreation.findall("condition"):
                listeFilters = [filtre for filtre in condition.findall("filter")]
                operatorType = condition.find("operator").get("type")

                if operatorType == "equals":
                    c = oRL.SQLCondition(
                        listeFilters[0].get("tableName").lower(),
                        listeFilters[0].get("columnName").lower(),
                        operatorType,
                        listeFilters[1].get("tableName").lower(),
                        listeFilters[1].get("columnName").lower(),
                    )
                    listeConditionsSQL.append(c.toString())
                if operatorType == "levenshteinDistance":
                    maximumDistance = condition.find("operator").get("maximumDistance")
                    c = oRL.SQLCondition(
                        listeFilters[0].get("tableName").lower(),
                        listeFilters[0].get("columnName").lower(),
                        operatorType,
                        listeFilters[1].get("tableName").lower(),
                        listeFilters[1].get("columnName").lower(),
                        maximumDistance,
                        None,
                    )
                    listeConditionsSQL.append(c.toString())
                if operatorType == "levenshteinSimilarity":
                    minimumSimilarity = condition.find("operator").get(
                        "minimumSimilarity"
                    )
                    c = oRL.SQLCondition(
                        listeFilters[0].get("tableName").lower(),
                        listeFilters[0].get("columnName").lower(),
                        operatorType,
                        listeFilters[1].get("tableName").lower(),
                        listeFilters[1].get("columnName").lower(),
                        None,
                        minimumSimilarity,
                    )
                    listeConditionsSQL.append(c.toString())
            sqlPairCreation = f"""
            DROP TABLE IF EXISTS paires_{i};

            CREATE TABLE paires_{i} as SELECT
            {self.listInputTables[0].primaryKeyName}::TEXT as pk0,
            {self.listInputTables[1].primaryKeyName}::TEXT as pk1,
            {self.listInputTables[0].primaryKeyName}::TEXT || {self.listInputTables[1].primaryKeyName}::TEXT as id,
            """
            for c in self.listInputTables[0].listeColummsRecordLinkage:
                sqlPairCreation += f"{self.listInputTables[0].tableName}.{c} as {c}_{self.listInputTables[0].tableName},"
            for c in self.listInputTables[1].listeColummsRecordLinkage:
                sqlPairCreation += f"{self.listInputTables[1].tableName}.{c} as {c}_{self.listInputTables[1].tableName},"
            sqlPairCreation = sqlPairCreation[
                :-1
            ]  
            sqlPairCreation += f" FROM {self.database.schema}.{self.listInputTables[0].tableName} , {self.database.schema}.{self.listInputTables[1].tableName} WHERE "

            for c in listeConditionsSQL:
                sqlPairCreation = sqlPairCreation + c + " AND "
            sqlPairCreation = sqlPairCreation[
                :-4
            ]  
            sqlPairCreation += ";"
            sqlQueriesPairCreation.append(sqlPairCreation)
            
        return sqlQueriesPairCreation

    def getNumberPairCreation(self):
        fuzzyRecordLinkage = self.root.find("fuzzyRecordLinkage")
        indexing = fuzzyRecordLinkage.find("indexing")
        return len(list(indexing.findall("pairCreation")))

    def getSqlQueryDoublesInPairsElimination(self):
        sqlQueryDoublesInPairsElimination = ""
        if self.numberPairCreation > 1:
            for j in range(2, self.numberPairCreation + 1):
                sqlQueryDoublesInPairsElimination += (
                    f" INSERT into paires_1 SELECT * from paires_{j};"
                )
                sqlQueryDoublesInPairsElimination += f"DROP table IF EXISTS paires_{j};"

        sqlQueryDoublesInPairsElimination += f"""
        CREATE table pairestemp as SELECT
        ROW_NUMBER() OVER (ORDER BY pk0) as id_seq,
        paires_1.*
        FROM paires_1;

        DROP table IF EXISTS paires_1;
        DROP table IF EXISTS paires;
        ALTER TABLE pairestemp RENAME TO paires;

        DELETE FROM paires a USING paires b
        WHERE a.id_seq < b.id_seq AND a.ID = b.ID;
        """
        return sqlQueryDoublesInPairsElimination

    def getListSimilaritiesCalculus(self):
        listSimilaritiesCalculus = []
        fuzzyRecordLinkage = self.root.find("fuzzyRecordLinkage")
        recordPairComparison = fuzzyRecordLinkage.find("recordPairComparison")
        for similarity in recordPairComparison.findall("similarity"):
            similarityName, similarityMethod = (
                similarity.get("similarityName"),
                similarity.get("similarityMethod"),
            )
            listeDicoParametreNomColumn = []
            for inputRL in similarity.findall("input"):
                dicoParametreNomColumn = {}
                tableName = inputRL.get("tableName").lower()
                if similarityMethod == "jaroWincklerSimilarityNames":
                    dicoParametreNomColumn["name1"] = (
                        inputRL.get("name1").lower() + "_" + tableName
                    )
                    try:
                        dicoParametreNomColumn["name2"] = (
                            inputRL.get("name2").lower() + "_" + tableName
                        )
                    except Exception as e:
                        None
                    try:
                        dicoParametreNomColumn["name3"] = (
                            inputRL.get("name3").lower() + "_" + tableName
                        )
                    except Exception as e:
                        None
                if similarityMethod == "levenshteinSimilarityBirthPlace":
                    dicoParametreNomColumn["BirthPlaceName"] = (
                        inputRL.get("BirthPlaceName").lower() + "_" + tableName
                    )
                if similarityMethod == "levenshteinSimilarityBirthPlaceCOG":
                    dicoParametreNomColumn["BirthPlaceCode"] = (
                        inputRL.get("BirthPlaceCode").lower() + "_" + tableName
                    )
                    dicoParametreNomColumn["BirthPlaceName"] = (
                        inputRL.get("BirthPlaceName").lower() + "_" + tableName
                    )
                if similarityMethod == "similarityBirthDate":
                    dicoParametreNomColumn["day"] = (
                        inputRL.get("day").lower() + "_" + tableName
                    )
                    dicoParametreNomColumn["month"] = (
                        inputRL.get("month").lower() + "_" + tableName
                    )
                    dicoParametreNomColumn["year"] = (
                        inputRL.get("year").lower() + "_" + tableName
                    )
                if similarityMethod == "binary":
                    dicoParametreNomColumn["columnName"] = (
                        inputRL.get("columnName").lower() + "_" + tableName
                    )
                listeDicoParametreNomColumn.append(dicoParametreNomColumn)
            listSimilaritiesCalculus.append(
                oRL.SimilarityCalculus(
                    similarityName, similarityMethod, listeDicoParametreNomColumn
                )
            )
        return listSimilaritiesCalculus

    def getListClassification(self):
        listClassification = []
        fuzzyRecordLinkage = self.root.find("fuzzyRecordLinkage")
        for classification in fuzzyRecordLinkage.findall("classification"):
            methodName = classification.get("methodName").lower()
            dicoParameters = {}
            if methodName == "handscore":
                dicoParameters["thresholdValue"] = classification.get("thresholdValue")
                dicoSimilarities = {}
                for similarityUsage in classification.findall("similarityUsage"):
                    similarityName = similarityUsage.get("similarityName")
                    wheight = similarityUsage.get("wheight")
                    if wheight is None:
                        wheight = 1
                    add = similarityUsage.get("add")
                    if add is None:
                        add = 0
                    power = similarityUsage.get("power")
                    if power is None:
                        power = 1
                    minimumValue = similarityUsage.get("minimumValue")
                    dicoSimilarities[similarityName] = oRL.SimilarityUsage(
                        wheight, add, power, minimumValue
                    )

                listClassification.append(
                    oRL.Classification(methodName, dicoParameters, dicoSimilarities)
                )

            if methodName == "randomforest":
                TreeNumberValue = classification.get("TreeNumberValue")
                if TreeNumberValue is None:
                    TreeNumberValue = 1000
                else:
                    TreeNumberValue = int(TreeNumberValue)
                dicoParameters["TreeNumberValue"] = TreeNumberValue
                dicoSimilarities = {}
                for similarityUsage in classification.findall("similarityUsage"):
                    similarityName = similarityUsage.get("similarityName")
                    dicoSimilarities[similarityName] = oRL.SimilarityUsage(
                        0, 0, 1
                    ) 
                listClassification.append(
                    oRL.Classification(methodName, dicoParameters, dicoSimilarities)
                )

            if methodName == "svm":
                kernel = classification.get("kernel").lower()
                if kernel is None:
                    kernel = "linear"
                dicoParameters["kernel"] = kernel
                C = classification.get("C")
                if C is None:
                    C = 0.01
                dicoParameters["C"] = float(C)
                dicoSimilarities = {}
                for similarityUsage in classification.findall("similarityUsage"):
                    similarityName = similarityUsage.get("similarityName")
                    dicoSimilarities[similarityName] = oRL.SimilarityUsage(
                        0, 0, 1
                    ) 
                listClassification.append(
                    oRL.Classification(methodName, dicoParameters, dicoSimilarities)
                )

        return listClassification

    def printSpecificationDetails(self):
        self.logger.info("printSpecificationDetails")
        self.logger.info(self.database)
        for inputTable in self.listInputTables:
            self.logger.info(inputTable)
        for k in self.dicoOutputTableNames:
            self.logger.info(k, self.dicoOutputTableNames[k])
        self.logger.info(self.classificationEvaluationTable)
        self.logger.info(self.sqlQueryExactRecordLinkage)
        for sqlPairCreation in self.sqlQueriesPairCreation:
            self.logger.info(sqlPairCreation)
        self.logger.info(self.sqlQueryDoublesInPairsElimination)
        for similarity in self.listSimilaritiesCalculus:
            self.logger.info(similarity)
        for classification in self.listClassification:
            self.logger.info(classification)

    def setValuesFromEnv(self, env_path: str):
        tree = ET.parse(self.cheminSpecificationXML)
        root = tree.getroot()
        server_parameter = root.find(".//serverParameter")
        user_parameter = root.find(".//userParameter")
        base_parameter = root.find(".//baseParameter")

        load_dotenv(env_path)

        self.user = os.environ.get('user')

        server_parameter.attrib.update({'server': os.environ.get('server'), 'port': os.environ.get('port')})
        user_parameter.attrib.update({'user': os.environ.get('user'), 'password': os.environ.get('password')})
        base_parameter.attrib.update({'database': os.environ.get('database'), 'schema': os.environ.get('schema')})

        tree.write(self.cheminSpecificationXML)