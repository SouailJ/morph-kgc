#Ce code a pour but de générer les triples issues du mapping dans le même dossier
import pandas as pd
import json
from jsonpath_ng.ext import parse as JSONPath
import urllib.request


import sys
import os
# Ajouter le chemin absolu vers le répertoire `src` au sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'C:/Users/souai/OneDrive/Bureau/Stage/morph-kgc/src')))

# Importer le module morph_kgc qui est dans le dossier src
import morph_kgc




def test_RMLTC():
    #Chemin d'accès à mapping.ttl
    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    print(mapping_path)
    
    #Fichier config
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'

    #Génération des triples issues du mapage
    triples = morph_kgc.materialize_set(config)
    print(triples)
    
    # Writing triples to output.nq
    output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq')
    with open(output_path, 'w', encoding='utf-8') as f:
        for triple in triples:
            f.write(f"{triple}\n")

if __name__ == "__main__":
    test_RMLTC()
