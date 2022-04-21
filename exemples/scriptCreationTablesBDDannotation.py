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
        drop table IF EXISTS annee_2017_2018.PAIRES_ANNOTATIONS;
        create table annee_2017_2018.PAIRES_ANNOTATIONS as 
            select pf.*,'OK' as statut_annotation 
            from annee_2017_2018.PAIRES_FINALES as pf
            where NOM_1_R_IJ_APPRENTI_CHAMP_SORTANT='MIDY';
        insert into annee_2017_2018.PAIRES_ANNOTATIONS 
            select pf.*,'KO' as statut_annotation 
            from annee_2017_2018.PAIRES_FINALES as pf
            where NOM_1_R_IJ_APPRENTI_CHAMP_SORTANT='BERNARDI';
        insert into annee_2017_2018.PAIRES_ANNOTATIONS 
            select pf.*,'OK' as statut_annotation 
            from annee_2017_2018.PAIRES_FINALES as pf
            where NOM_1_R_IJ_APPRENTI_CHAMP_SORTANT='TESTD';
        """
        )
connection.close()