# -*- coding: utf-8 -*-
import specificationAppariement as spec
import os

if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cheminXML = os.path.join(dir_path, "specificationAppariement.xml")
    cheminEnv = os.path.join(dir_path, ".env")

    specificationAppariement = spec.SpecificationAppariement(cheminXML, cheminEnv)
    specificationAppariement.runSpecification()
