import sqlalchemy as sqla
import logging


class classificationEvaluationTable:
    def __init__(self, tableName, annotationColumnName, dicoModalites):
        self.tableName = tableName
        self.annotationColumnName = annotationColumnName
        self.dicoModalites = dicoModalites

    def __str__(self):
        resultat = "classificationEvaluationTable : \n"
        resultat += f"tableName:{self.tableName}"
        resultat += f"annotationColumnName:{self.annotationColumnName}"
        for k in self.dicoModalites:
            resultat += f"{k}:{self.dicoModalites[k]}"
        return resultat


class SpecificationExecutionParameters:
    def __init__(
        self,
        cheminSpecificationXML,
        dateDerniereModificationXML,
        isDerniereExecutionOK,
        executeAll,
    ):
        self.cheminSpecificationXML = cheminSpecificationXML
        self.dateDerniereModificationXML = dateDerniereModificationXML
        self.isDerniereExecutionOK = isDerniereExecutionOK
        self.executeAll = executeAll

    def __str__(self):
        resultat = "SpecificationExecutionParameters : \n"
        resultat += f"cheminSpecificationXML:{self.cheminSpecificationXML} \n"
        resultat += f"dateDerniereModificationXML:{self.dateDerniereModificationXML} \n"
        resultat += f"isDerniereExecutionOK:{self.isDerniereExecutionOK} \n"
        resultat += f"executeAll:{self.executeAll} \n"
        return resultat


class XMLchecks:
    def __init__(self, isSpecificationOK, listeXMLerror, listeXMLWarnings):
        self.isSpecificationOK = isSpecificationOK
        self.listeXMLerror = listeXMLerror
        self.listeXMLWarnings = listeXMLWarnings

    def __str__(self):
        resultat = ""
        if len(self.listeXMLerror) > 0:
            resultat = "ERROR(s) list : \n"
            for error in self.listeXMLerror:
                resultat += error
            resultat += "\n"
        if len(self.listeXMLWarnings) > 0:
            resultat += "WARNING(s) list : \n"
            for warning in self.listeXMLWarnings:
                resultat += warning
            resultat += "\n"
        return resultat


class Database:
    def __init__(self, server, port, user, password, database, schema, logger):
        self.server = server
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.logger = logger
        try:
            self.engine = sqla.create_engine(
                f"postgresql://{self.user}:{self.password}@{self.server}:{self.port}/{self.database}"
            )
        except Exception as exception:
            self.logger.error(exception)

    def __str__(self):
        resultat = f"server:{self.server} port:{self.port}\n"
        resultat = resultat + f"user:{self.user} password:{self.password}\n"
        resultat = resultat + f"database:{self.database} schema:{self.schema}\n"
        return resultat


class Table:
    def __init__(self, tableName, primaryKeyName, listeColummsRecordLinkage):
        self.tableName = tableName
        self.primaryKeyName = primaryKeyName
        self.listeColummsRecordLinkage = listeColummsRecordLinkage

    def __str__(self):
        resultat = f"tableName:{self.tableName} primaryKeyName:{self.primaryKeyName}\n"
        for column in self.listeColummsRecordLinkage:
            resultat = resultat + f"{column}\n"
        return resultat


class SQLCondition:
    def __init__(
        self,
        tableNameA,
        columnNameA,
        operatorType,
        tableNameB,
        columnNameB,
        maximumDistance=None,
        minimumSimilarity=None,
    ):
        self.tableNameA = tableNameA
        self.columnNameA = columnNameA
        self.operatorType = operatorType
        self.tableNameB = tableNameB
        self.columnNameB = columnNameB
        self.maximumDistance = maximumDistance
        self.minimumSimilarity = minimumSimilarity

    def toString(self):
        if self.operatorType == "equals":
            return f"{self.tableNameA}.{self.columnNameA}={self.tableNameB}.{self.columnNameB}"
        if self.operatorType == "levenshteinDistance":
            return f"levenshtein({self.tableNameA}.{self.columnNameA},{self.tableNameB}.{self.columnNameB})<{self.maximumDistance}"
        if self.operatorType == "levenshteinSimilarity":
            return f"levenshtein_similarity({self.tableNameA}.{self.columnNameA},{self.tableNameB}.{self.columnNameB})>{self.minimumSimilarity}"


class SimilarityCalculus:
    def __init__(self, similarityName, similarityMethod, listeDicoParametreNomColumn):
        self.similarityName = similarityName
        self.similarityMethod = similarityMethod
        self.listeDicoParametreNomColumn = listeDicoParametreNomColumn

    def __str__(self):
        resultat = f"similarityName:{self.similarityName} similarityMethod:{self.similarityMethod}\n"
        for dico in self.listeDicoParametreNomColumn:
            for k in dico:
                resultat += f"{k}:{dico[k]}\n"
        return resultat


class Classification:
    def __init__(self, methodName, dicoParameters, dicoSimilarities=None):
        self.methodName = methodName
        self.dicoParameters = dicoParameters
        self.dicoSimilarities = dicoSimilarities

    def __str__(self):
        resultat = f"methodName:{self.methodName}\n"
        for k in self.dicoParameters:
            resultat += f"key:{k} value:{self.dicoParameters[k]}\n"
        if self.methodName == "handScore":
            for k in self.dicoSimilarities:
                resultat += f"key:{k} value:{self.dicoSimilarities[k]}\n"
        return resultat


class SimilarityUsage:
    def __init__(self, wheight, add=0, power=1, minimumValue=None):
        self.wheight = wheight
        self.add = add
        self.power = power
        self.minimumValue = minimumValue

    def __str__(self):
        return f"wheight:{self.wheight} add:{self.add} power:{self.power} minimumValue:{self.minimumValue}"
