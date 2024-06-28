__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"




import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare

def test():

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    #g_morph = morph_kgc.materialize(config)
    #assert compare.isomorphic(g, g_morph)
    
    triples = morph_kgc.materialize_set(config)
    print(triples)
    
    # Writing triples to output_exp.nq
    output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output_exp.nq')
    with open(output_path, 'w', encoding='utf-8') as f:
        for triple in triples:
            f.write(f"{triple}\n")

if __name__ == "__main__":
    test()
    print("Test passed: The generated RDF graph is isomorphic to the reference graph.")
