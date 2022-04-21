# -*- coding: utf-8 -*-
import sqlalchemy as sqla
import os
from dotenv import load_dotenv

#variables d'environnement
load_dotenv()

#paramètres connexion BDD
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
server = os.environ.get("SERVER")
port = os.environ.get("PORT")
database = os.environ.get("DATABASE")

#connection BDD
engine = sqla.create_engine(f"postgresql://{user}:{password}@{server}:{port}/{database}")

#similariteLevenshtein =1-levenshtein(a,b)/max(length(a),len(b))
connection = engine.connect()
connection.execute(        
        """
		DROP FUNCTION IF EXISTS public.levenshtein_similarity;

		CREATE FUNCTION public.levenshtein_similarity(IN string_a text, IN string_b text)
			RETURNS numeric
			LANGUAGE 'plpgsql'
			AS $BODY$BEGIN
				return 1- CAST(levenshtein(string_a,string_b) as NUMERIC)/CAST(greatest(length(string_a),length(string_b)) as NUMERIC);
			END;$BODY$;

		ALTER FUNCTION public.levenshtein_similarity(text, text)
			OWNER TO {f_user};
		""".format(f_user = user)
        )
connection.close()