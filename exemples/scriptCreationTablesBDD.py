# -*- coding: utf-8 -*-
import sqlalchemy as sqla
import os
from dotenv import load_dotenv

load_dotenv()

user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
server = os.environ.get("SERVER")
port = os.environ.get("PORT")
database = os.environ.get("DATABASE")


engine = sqla.create_engine(f"postgresql://{user}:{password}@{server}:{port}/{database}")
connection = engine.connect()
connection.execute(
        """
        DROP SCHEMA IF EXISTS annee_2017_2018 CASCADE;
        CREATE SCHEMA annee_2017_2018;
        
        drop table IF EXISTS annee_2017_2018.IJ_APPRENTI_CHAMP_SORTANT;
        CREATE TABLE annee_2017_2018.IJ_APPRENTI_CHAMP_SORTANT(
        	ID_INE_RNIE VARCHAR (11) UNIQUE NOT NULL PRIMARY KEY,
        	NOM_1_R VARCHAR (50),
        	NOM_2_R VARCHAR (50),
        	PRENOM_1_R VARCHAR (50),
        	PRENOM_2_R VARCHAR (50),
        	PRENOM_3_R VARCHAR (50),
        	JOUR_NAISSANCE INTEGER,
        	MOIS_NAISSANCE INTEGER,
        	ANNEE_NAISSANCE INTEGER,
        	SEXE VARCHAR (1),
            ID_COG_COMMUNE_NAISSANCE VARCHAR (5),	
        	ID_COG_DEP_NAISSANCE VARCHAR (2),
         	LIBELLE_COMMUNE_NAISSANCE VARCHAR (50)
            );
        grant all on annee_2017_2018.IJ_APPRENTI_CHAMP_SORTANT to {};

        drop table IF EXISTS annee_2017_2018.IJ_SALARIE_TEMPORAIRE;
        CREATE TABLE annee_2017_2018.IJ_SALARIE_TEMPORAIRE(
        	ID_SISMMO VARCHAR (11) UNIQUE NOT NULL PRIMARY KEY,
        	NOM_1_R VARCHAR (50),
        	NOM_2_R VARCHAR (50),
        	PRENOM_1_R VARCHAR (50),
        	PRENOM_2_R VARCHAR (50),
        	PRENOM_3_R VARCHAR (50),
        	JOUR_NAISSANCE INTEGER,
        	MOIS_NAISSANCE INTEGER,
        	ANNEE_NAISSANCE INTEGER,
        	SEXE VARCHAR (1),
            ID_COG_COMMUNE_NAISSANCE VARCHAR (5),	
        	ID_COG_DEP_NAISSANCE VARCHAR (2),
         	LIBELLE_COMMUNE_NAISSANCE VARCHAR (50)
            );
        grant all on annee_2017_2018.IJ_SALARIE_TEMPORAIRE to {};
        
        INSERT INTO annee_2017_2018.IJ_APPRENTI_CHAMP_SORTANT
            VALUES ('00000000001','MIDY',null,'LOIC',null,null,2,8,1977,1,92032,92,'FONTENAY AUX ROSES');
        INSERT INTO annee_2017_2018.IJ_APPRENTI_CHAMP_SORTANT
            VALUES ('00000000002','BERNARDI','MIDY','SOPHIE',null,null,11,8,1979,1,92032,92,'FONTENAY AUX ROSES');
        INSERT INTO annee_2017_2018.IJ_APPRENTI_CHAMP_SORTANT
            VALUES ('00000000003','TEST','MIDY','SOPHIE',null,null,11,8,1979,1,92032,92,'FONTENAY AUX ROSES');
        
        INSERT INTO annee_2017_2018.IJ_SALARIE_TEMPORAIRE
            VALUES ('00000000001','MIDY',null,'LOIC',null,null,1,8,1977,1,92032,92,'FONTENAY AUX ROSES');
        INSERT INTO annee_2017_2018.IJ_SALARIE_TEMPORAIRE
            VALUES ('00000000002','BERNARDINI','MIDY','SOPHIE',null,null,11,8,1979,1,92032,92,'FONTENAY AUX ROSES');
        INSERT INTO annee_2017_2018.IJ_SALARIE_TEMPORAIRE
            VALUES ('00000000003','TESTD','MIDY','SOPHIE',null,null,11,8,1979,1,92032,92,'FONTENAY AUX ROSES');
        """.format(user, user)
        )
connection.close()