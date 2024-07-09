import sys
import os

# Get the absolute path of the directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..', 'src')))

# Import the morph_kgc module from the src folder
import morph_kgc

def test_RMLTC():
    #path to mapping.ttl    
    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    
    #Config file
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'

    #Generation of triples from mapping
    triples = morph_kgc.materialize_set(config)
    print(triples)
    
    # Writing triples to output.nq
    output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq')
    with open(output_path, 'w', encoding='utf-8') as f:
        for triple in triples:
            f.write(f"{triple}\n")

if __name__ == "__main__":
    test_RMLTC()
