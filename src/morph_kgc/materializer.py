__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

import uuid
import numpy as np
from rdflib import URIRef, Literal, RDF

from falcon.uri import encode_value
from urllib.parse import quote

from .utils import *
from .constants import *
from .data_source.relational_db import get_sql_data
from .data_source.property_graph_db import get_pg_data
from .data_source.data_file import get_file_data
#
from .data_source.data_file import load_json
from .data_source.data_file import check_for_empty_lists

from .data_source.python_data import get_ram_data
from .fnml.fnml_executer import execute_fnml


def _add_references_in_join_condition(rml_rule, references, parent_references):
    references_join, parent_references_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')

    references.update(set(references_join))
    parent_references.update(set(parent_references_join))

    return references, parent_references


def _preprocess_data(data, rml_rule, references, config):
    # deal with ORACLE
    if rml_rule['source_type'] == RDB:
        if config.get_db_url(rml_rule['source_name']).lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)

    # TODO: can this be removed?
    data = data.map(str)

    data = remove_null_values_from_dataframe(data, config, references)
    data = data.convert_dtypes(convert_boolean=False)

    # data to str
    data = data.astype(str)

    # remove duplicates
    data = data.drop_duplicates()

    return data


def _get_data(config, rml_rule, references, python_source=None):
    if rml_rule['source_type'] == RDB:
        data = get_sql_data(config, rml_rule, references)
    elif rml_rule['source_type'] == PGDB:
        data = get_pg_data(config, rml_rule, references)
    elif rml_rule['source_type'] in FILE_SOURCE_TYPES:
        data = get_file_data(rml_rule, references)
    elif rml_rule['source_type'] in IN_MEMORY_TYPES:
        data = get_ram_data(rml_rule, references, python_source)

    data = _preprocess_data(data, rml_rule, references, config)

    return data


def _get_references_in_rml_rule(rml_rule, rml_df, fnml_df, only_subject_map=False):
    references = []

    positions = ['subject'] if only_subject_map else ['subject', 'predicate', 'object', 'graph', 'lang_datatype']
    for position in positions:
        if rml_rule[f'{position}_map_type'] == RML_TEMPLATE:
            references.extend(get_references_in_template(rml_rule[f'{position}_map_value']))
        elif rml_rule[f'{position}_map_type'] == RML_REFERENCE:
            references.append(rml_rule[f'{position}_map_value'])
        elif rml_rule[f'{position}_map_type'] == RML_EXECUTION:
            references.extend(get_references_in_fnml_execution(fnml_df, rml_rule[f'{position}_map_value']))
    
    #Ajout : Si on a un gather map dans le mapping, on ajoute le "label" (nom de la colonne) des données dont on aura besoin
    
    if pd.notna(rml_rule['gather']):
        print("references", references)
        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        for ref in refs:
            print ("ref", ref)
            references.append(ref)
        # references.append(rml_rule['gather_reference']) #Ancienne version 


    # term maps with join conditions (referencing and quoted)
    positions = ['subject'] if only_subject_map else ['subject', 'object']
    for position in positions:
        if rml_rule[f'{position}_map_type'] == RML_QUOTED_TRIPLES_MAP and pd.isna(rml_rule[f'{position}_join_conditions']):
            parent_rml_rule = get_rml_rule(rml_df, rml_rule[f'{position}_map_value'])
            references.extend(_get_references_in_rml_rule(parent_rml_rule, rml_df, fnml_df))

        references_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, f'{position}_join_conditions')
        references.extend(references_join)

    print("refereNCES", references)
    return references


def _materialize_template(results_df, template, expression_type, config, position, columns_alias='', termtype='', datatype=''):
    if expression_type == RML_REFERENCE:
        # convert RML reference to template
        template = f'{{{template}}}'

    references = get_references_in_template(template)


    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')
    # formatting according to the termtype is done at the end
    results_df[position] = ''

    for reference in references:
        results_df['reference_results'] = results_df[columns_alias + reference]

        if config.only_write_printable_characters():
            results_df['reference_results'] = results_df['reference_results'].apply(
                lambda x: remove_non_printable_characters(x))

        if termtype.strip() == RML_IRI and expression_type == RML_TEMPLATE:
            if config.get_safe_percent_encoding():
                results_df['reference_results'] = results_df['reference_results'].apply(
                    lambda x: quote(x, safe=config.get_safe_percent_encoding()))
            else:
                results_df['reference_results'] = results_df['reference_results'].apply(lambda x: encode_value(x))
        elif termtype.strip() == RML_LITERAL:
            # Natural Mapping of SQL Values (https://www.w3.org/TR/r2rml/#natural-mapping)
            if datatype == XSD_BOOLEAN:
                results_df['reference_results'] = results_df['reference_results'].str.lower()
            elif datatype == XSD_DATETIME:
                results_df['reference_results'] = results_df['reference_results'].str.replace(' ', 'T', regex=False)
                # Make integers not end with .0
            elif datatype == XSD_INTEGER:
                results_df['reference_results'] = results_df['reference_results'].astype(float).astype(int).astype(str)

            # TODO: this can be avoided for most cases (if '\\' in data_value)
            results_df['reference_results'] = results_df['reference_results'].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)

        splitted_template = template.split('{' + reference + '}')
        results_df[position] = results_df[position] + splitted_template[0] + results_df['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        results_df[position] = results_df[position] + template

    if termtype.strip() == RML_IRI:
        results_df[position] = '<' + results_df[position] + '>'
    elif termtype.strip() == RML_BLANK_NODE:
        results_df[position] = '_:' + results_df[position]
    elif termtype.strip() == RML_LITERAL:
        results_df[position] = '"' + results_df[position] + '"'
    else:
        # this case is for language and datatype maps, do nothing
        pass

    return results_df


def _materialize_fnml_execution(results_df, fnml_execution, fnml_df, config, position, termtype=RML_LITERAL, datatype=''):
    results_df = execute_fnml(results_df, fnml_df, fnml_execution, config)

    if config.only_write_printable_characters():
        results_df[fnml_execution] = results_df[fnml_execution].apply(lambda x: remove_non_printable_characters(x))

    if termtype.strip() == RML_LITERAL:
        # Natural Mapping of SQL Values (https://www.w3.org/TR/r2rml/#natural-mapping)
        if datatype == XSD_BOOLEAN:
            results_df[fnml_execution] = results_df[fnml_execution].str.lower()
        elif datatype == XSD_DATETIME:
            results_df[fnml_execution] = results_df[fnml_execution].str.replace(' ', 'T', regex=False)
        # Make integers not end with .0
        elif datatype == XSD_INTEGER:
            results_df[fnml_execution] = results_df[fnml_execution].astype(float).astype(int).astype(str)

        results_df[fnml_execution] = results_df[fnml_execution].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)
        results_df[position] = '"' + results_df[fnml_execution] + '"'
    elif termtype.strip() == RML_IRI:
        # it is assumed that the IRI values will be correct, and they are not percent encoded
        results_df[fnml_execution] = results_df[fnml_execution].apply(lambda x: x.strip())
        results_df[position] = '<' + results_df[fnml_execution] + '>'
    elif termtype.strip() == RML_BLANK_NODE:
        results_df[position] = '_:' + results_df[fnml_execution]

    return results_df


def _materialize_rml_rule_terms(results_df, rml_rule, fnml_df, config, columns_alias=''):
    if rml_rule['subject_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        results_df = _materialize_template(results_df, rml_rule['subject_map_value'], rml_rule['subject_map_type'], config, 'subject',
                                           termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == RML_EXECUTION:
        results_df = _materialize_fnml_execution(results_df, rml_rule['subject_map_value'], fnml_df, config, 'subject',
                                                 termtype=rml_rule['subject_termtype'])
    if rml_rule['predicate_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        results_df = _materialize_template(results_df, rml_rule['predicate_map_value'], rml_rule['predicate_map_type'], config, 'predicate', termtype=RML_IRI)
    elif rml_rule['predicate_map_type'] == RML_EXECUTION:
        results_df = _materialize_fnml_execution(results_df, rml_rule['predicate_map_value'], fnml_df, config,
                                                 'predicate', termtype=RML_IRI)
    if rml_rule['object_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        results_df = _materialize_template(results_df, rml_rule['object_map_value'], rml_rule['object_map_type'], config, 'object',
                                           columns_alias=columns_alias, termtype=rml_rule['object_termtype'], datatype=rml_rule['lang_datatype_map_value'])
    elif rml_rule['object_map_type'] == RML_EXECUTION:
        results_df = _materialize_fnml_execution(results_df, rml_rule['object_map_value'], fnml_df, config, 'object',
                                                 termtype=rml_rule['object_termtype'], datatype=rml_rule['lang_datatype_map_value'])

    if rml_rule['lang_datatype'] == RML_LANGUAGE_MAP:
        if rml_rule['lang_datatype_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
            results_df = _materialize_template(results_df, rml_rule['lang_datatype_map_value'], rml_rule['lang_datatype_map_type'],
                                               config, 'lang_datatype')
        elif rml_rule['lang_datatype_map_type'] == RML_EXECUTION:
            results_df = _materialize_fnml_execution(results_df, rml_rule['lang_datatype_map_value'], fnml_df, config,
                                                     'lang_datatype')
        results_df['object'] = results_df['object'] + '@' + results_df['lang_datatype']
    elif rml_rule['lang_datatype'] == RML_DATATYPE_MAP:
        if rml_rule['lang_datatype_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
            results_df = _materialize_template(results_df, rml_rule['lang_datatype_map_value'], rml_rule['lang_datatype_map_type'],
                                               config, 'lang_datatype', termtype=RML_IRI)
        elif rml_rule['lang_datatype_map_type'] == RML_EXECUTION:
            results_df = _materialize_fnml_execution(results_df, rml_rule['lang_datatype_map_value'], fnml_df, config,
                                                     'lang_datatype', termtype=RML_IRI)
        results_df['object'] = results_df['object'] + '^^' + results_df['lang_datatype']

    return results_df


#Ajout : cette fonction fait une concaténation de 2 dataframes pour que le template soit à la fois object d'un triple et subject d'un autre triple
def _materialize_rml_rule_terms_named_cc(results_df, rml_rule, fnml_df, config, columns_alias=''):
    
    #Dataframes qui contiendront les triples
    
    # df contient les triples avec le template en object
    # df2 contient les triples avec le template en subject (liste par exemple)


    print("results_df",results_df)#Trouver les modifs à réaliser pour results_df
    #Permet de modifier results_df pour gérer les cas avec plusieurs rml:reference dans le rml:gather
    if pd.notna(rml_rule['gather']):
        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        #print("len(refs)", len(refs))

        if len(refs) > 1:
            print("0")
            references = get_references_in_template(rml_rule['subject_map_value'])
            print("references[0]", references[0])
        
            # # Définition des références
            references = [references[0]]
            # refs = ['vala', 'valb']

            # Fusionner les colonnes spécifiées dans refs en une seule colonne 'val'
            melted_df = pd.melt(results_df, id_vars=references, value_vars=refs, var_name='variable', value_name='val')

            # Suppression de la colonne 'variable' inutile après la fusion
            melted_df = melted_df.drop(columns=['variable'])

            # Suppression des doublons
            melted_df = melted_df.drop_duplicates().reset_index(drop=True)

            # Tri des valeurs pour assurer que les ids sont regroupés
            melted_df = melted_df.sort_values(by=references + ['val']).reset_index(drop=True)

            results_df=melted_df
            rml_rule['gather_reference']='val'



    # print("melted_df",melted_df)#Trouver les modifs à réaliser pour results_df
    # print("results_df",results_df)#Trouver les modifs à réaliser pour results_df

    df=pd.DataFrame(results_df)
    df2=pd.DataFrame()

    

    #Remplissage du dataframe df (extrait de la fonction _materialize_rml_rule_terms) 
    if rml_rule['subject_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        df = _materialize_template(df, rml_rule['subject_map_value'], rml_rule['subject_map_type'], config, 'subject',
                                           termtype=rml_rule['subject_termtype'])

    if rml_rule['predicate_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        df = _materialize_template(df, rml_rule['predicate_map_value'], rml_rule['predicate_map_type'], config, 'predicate', termtype=RML_IRI)

    if rml_rule['object_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        df = _materialize_template(df, rml_rule['object_map_value'], rml_rule['object_map_type'], config, 'object',
                                           columns_alias=columns_alias, termtype=rml_rule['object_termtype'], datatype=rml_rule['lang_datatype_map_value'])
    
    print("df",df )
    #print ("results_df", results_df.columns, results_df)

    #TODO --> METTRE DANS LE IF LIST OU LE IF CONTAINER
    
    # refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
    # for ref in refs: #Parcourt tous les rml:reference compris dans un rml:gather
    #     print ("ref", ref)



    #     json_data=load_json(rml_rule)
    #     print("json_data", json_data)
    #     check_list=check_for_empty_lists(json_data, ref)
    #     references = get_references_in_template(rml_rule['object_map_value']) #Pas besoin de changer car cela concerne seulement le objectmap
    #     print("REFERENCES", references)#--> id
    #     print("check_list", check_list)
    
    
    # Cas d'une named List
    if rml_rule['gatherAs'] in [RDF_LIST]:  #rml_rule['object_map_type'] in [RML_TEMPLATE] and 



        json_data=load_json(rml_rule)
        check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
        references = get_references_in_template(rml_rule['subject_map_value'])

        print("rml_gather", rml_rule['gather'])

        # if pd.notna(rml_rule['gather']):
        #     refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        #     print(refs)
        
        #Cette partie complète convenablement le 1er dataframe lorsque les listes vides sont autorisées     
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true" : #and len(refs)<=1:
            print("pas vide")
            # Traiter le dataframe pour ajouter des lignes avec NaN pour les listes vides
            additional_rows = []
            for item in json_data:
                
                if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
                    subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                    object_empty=rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
            
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"})
                    #additional_rows.append({ 'id': item['id'], 'subject': rml_rule['subject_map_value'], 'predicate':"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt>" })

            # Ajouter les nouvelles lignes au dataframe
            if additional_rows:
                df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)

        # elif check_list==True and len(refs)>1:
        #     print("vide")

        #     # Traiter le dataframe pour ajouter des lignes avec NaN pour les listes vides
        #     additional_rows = []
        #     for item in json_data:
        #         for ref in refs:
        #             if rml_rule[ref] in item and isinstance(item[rml_rule[ref]], list) and len(item[rml_rule[ref]]) == 0:
        #                 subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
        #                 object_empty=rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                
        #                 additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"})
        #                 #additional_rows.append({ 'id': item['id'], 'subject': rml_rule['subject_map_value'], 'predicate':"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt>" })

        #     # Ajouter les nouvelles lignes au dataframe
        #     if additional_rows:
        #         df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)



            #print("new df", df)








        #Partie 2 : remplir df2 comme une liste

        #Permet de récupérer les noms des colonnes "utiles" pour le template dans la dataframe references
        references = get_references_in_template(rml_rule['object_map_value'])
        
        # print("REFERENCES",references, "REFERENCES[0]", references[0])
        

        #On définit la valeur du subject (au départ)
        subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[0, references[0]]))

        #Blank nodes que l'on incrémente grâce à bnode_counter
        bnode_counter=1
        bnode = f"_:bnode{bnode_counter}"

        #
        print("results_df", results_df)
        print("rml_rule['gather_reference']", rml_rule['gather_reference'])
        print("len", len(results_df[rml_rule['gather_reference']])) 
        
        # if rml_rule[0,'gather_reference'] est nul et rmlallow.. == true 

        # Boucle parcourant le dataframe contenant les données 
        for i in range(len(results_df[rml_rule['gather_reference']])):
            
            if i < len(results_df) - 1:
                #si la value actuelle et celle d'après ont le même id --> début ou suite d'une liste --> Le prochain subject sera une blank node
                if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                    if subject_value==bnode:
                        bnode = f"_:bnode{bnode_counter}"

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                            'object': f"{bnode}"
                            }, ignore_index=True)
                        subject_value = bnode                        
                        #bnode = f"_:bnode{bnode_counter}"
                        bnode_counter += 1        

                    else:
                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                            'object': f"{bnode}"
                            }, ignore_index=True)
                        
                        subject_value = bnode                        
                        bnode = f"_:bnode{bnode_counter}"
                        bnode_counter += 1  
    
                #si la value actuelle et celle d'après n'ont PAS le même id --> fin d'une liste --> Le prochain subject sera un template
                elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: #or results_df.loc[i+1, 'id'] == None:
                    if subject_value==bnode:
                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                            }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                            'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
                            }, ignore_index=True)
                        
                        subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[i+1, references[0]]))
                        bnode_counter += 1      
                        bnode = f"_:bnode{bnode_counter}"

                        
                    else:
                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                            }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                            'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
                            }, ignore_index=True)
                        subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[i+1, references[0]]))
                        bnode_counter += 1      
                        bnode = f"_:bnode{bnode_counter}"

            #la value actuelle est la dernière du dataframe --> fin de la liste ou début d'une nouvelle liste d'un élément
            else:
                if subject_value==bnode:
                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                        'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
                        }, ignore_index=True)
                else: 
                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                        'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
                        }, ignore_index=True)
                      
        # print("df", df)
        # print("df2", df2)
        #print("rml_rule['gather_node']", rml_rule['gather_node'])

        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)

        # print("df_final", df_final)

    
    #Collection (Bag, Alt ou Seq)
    elif rml_rule['gatherAs'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:
        #
        json_data=load_json(rml_rule)
        #print("json_data", json_data)
        check_list=check_for_empty_lists(json_data, rml_rule['gather_reference']) #ref
        references = get_references_in_template(rml_rule['object_map_value']) #Pas besoin de changer car cela concerne seulement le objectmap
        # print("REFERENCES", references)#--> id
        # print("check_list", check_list)

        # print("df000000000000",df) #doit contenir vala et valb




        #Cette partie complète convenablement le 1er dataframe lorsque les listes vides sont autorisées     
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
            # Traiter le dataframe pour ajouter des lignes avec NaN pour les listes vides
            additional_rows = []
            for item in json_data:
                #print("item", item)
                subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                object_empty=rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"<{object_empty}>" })
                    #additional_rows.append({ 'id': item['id'], 'subject': rml_rule['subject_map_value'], 'predicate':"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt>" })


            # Ajouter les nouvelles lignes au dataframe
            if additional_rows:
                df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)

            #print("df",df)

        #/////////////////////////////
        

        #Partie2
        count=1
        references = get_references_in_template(rml_rule['object_map_value'])
        #print("REFERENCES",references, "REFERENCES[0]", references[0]) references[0] --> id


        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
            # Identification des lignes manquantes entre df et results_df            
            merged_df = df.merge(results_df, on=[references[0], rml_rule['gather_reference']], how='left', indicator=True)
            missing_rows = merged_df[merged_df['_merge'] == 'left_only'][[references[0], rml_rule['gather_reference']]]

            # Ajout des lignes manquantes à results_df
            results_df = results_df._append(missing_rows, ignore_index=True)


        #On définit la valeur du subject (au départ)
        subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[0, references[0]]))
 
        #Détermine quel container on a 
        if rml_rule['gatherAs'] == RDF_BAG:
            collection = 'Bag'
        elif rml_rule['gatherAs'] == RDF_SEQ:
            collection = 'Seq'
        elif rml_rule['gatherAs'] == RDF_ALT:
            collection = 'Alt'

       


        # Boucle parcourant le dataframe contenant les données 
        for i in range(len(results_df[rml_rule['gather_reference']])):

            if i < len(results_df) - 1:
                #si la value actuelle et celle d'après ont le même id --> début ou suite d'un container --> Le prochain subject sera le même que le sujet actuel (template)
                if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                    # bnode = f"_:bnode{bnode_counter}"

                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                    }, ignore_index=True)

                    count+=1

                #si la value actuelle et celle d'après n'ont PAS le même id --> fin d'un container --> Le prochain subject sera un template différent
                elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: #or results_df.loc[i+1, 'id'] == None:

                    #Container vide
                    if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                        }, ignore_index=True)

                    #Container non vide
                    else:
                        
                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                            }, ignore_index=True)

                    count=1    

                    print("rml_rule['gather_reference']", rml_rule['gather_reference'])

                    #Définition du nouveau sujet pour le prochain triple 
                    subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[i+1, references[0]]))
                    print("new subject", subject_value)

            #la value actuelle est la dernière du dataframe --> fin du container
            else:
                #Container vide
                if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):
                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                        }, ignore_index=True)
                #Container non vide
                else:
                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)
                    
                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                         }, ignore_index=True)
                    

        
        # print("df", df)
        # print("df2", df2)

        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)


    else:
        df_final = df


    return df_final
   
#Ajout : cette fonction fait une concaténation de 2 dataframes pour que le template soit à la fois object d'un triple et subject d'un autre triple
def _materialize_rml_rule_terms_unnamed_cc(results_df, rml_rule, fnml_df, config, columns_alias=''):



    print("results_df",results_df)#Trouver les modifs à réaliser pour results_df
    #Permet de modifier results_df pour gérer les cas avec plusieurs rml:reference dans le rml:gather
    if pd.notna(rml_rule['gather']):
        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        #print("len(refs)", len(refs))

        if len(refs) > 1:
            print("0")
            references = get_references_in_template(rml_rule['subject_map_value'])
            print("references[0]", references[0])
        
            # # Définition des références
            references = [references[0]]
            # refs = ['vala', 'valb']

            # Fusionner les colonnes spécifiées dans refs en une seule colonne 'val'
            melted_df = pd.melt(results_df, id_vars=references, value_vars=refs, var_name='variable', value_name='val')

            # Suppression de la colonne 'variable' inutile après la fusion
            melted_df = melted_df.drop(columns=['variable'])

            # Suppression des doublons
            melted_df = melted_df.drop_duplicates().reset_index(drop=True)

            # Tri des valeurs pour assurer que les ids sont regroupés
            melted_df = melted_df.sort_values(by=references + ['val']).reset_index(drop=True)

            results_df=melted_df
            rml_rule['gather_reference']='val'







    # df contient les triples avec le subject, predicate et une blank node en oject
    # df2 contient les triples avec la même blank node que précédemment en subject (pour une liste par exemple)
    df=pd.DataFrame(results_df)
    df2=pd.DataFrame()
    
    #Remplissage du dataframe df (extrait de la fonction _materialize_rml_rule_terms) 
    if rml_rule['subject_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        df = _materialize_template(df, rml_rule['subject_map_value'], rml_rule['subject_map_type'], config, 'subject',
                                           termtype=rml_rule['subject_termtype'])

    if rml_rule['predicate_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        df = _materialize_template(df, rml_rule['predicate_map_value'], rml_rule['predicate_map_type'], config, 'predicate', termtype=RML_IRI)

    #Création d'une colonne "object" (vide) pour ce dataframe 
    df['object'] = None

    
    # print("len", len(results_df[rml_rule['gather_reference']]))
    
    #Permet de récupérer les noms des colonnes "utiles" dans lesquelles on a les données (après rml:gather)
    references = get_references_in_template(rml_rule['subject_map_value'])

    #Blank nodes pour chaque subjectMap que l'on incrémente grâce à bnode_counter_subject
    bnode_counter_subject=1
    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
    
    # print("references", references)
    # print("references[0]", references[0])

    # Boucle parcourant les lignes du dataframe contenant les données 
    for i in range(len(results_df[rml_rule['gather_reference']])):
        if i < len(results_df) - 1:

            #si la value actuelle et celle d'après ont le même id --> début ou suite d'une liste 
            # --> L'object sera une blank node (la même que précédemment si l'on est déjà passé par cette condition if)
            if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                df.loc[i, 'object'] = bnode_subject
            
            #si la value actuelle et celle d'après n'ont PAS le même id --> fin d'une liste --> Le prochain object sera une AUTRE blank node (incrémentation)
            elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]:
                df.loc[i, 'object'] = bnode_subject
                bnode_counter_subject +=1
                bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

        #la value actuelle est la dernière du dataframe --> fin de la liste
        else:
            df.loc[i, 'object'] = bnode_subject

    # print("df", df)

    
    # json_data=load_json(rml_rule)
    # check_list=check_for_empty_lists(json_data)
    # references = get_references_in_template(rml_rule['subject_map_value'])


    # #Cette partie complète convenablement le 1er dataframe lorsque les listes vides sont autorisées     
    # if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
    #     # Traiter le dataframe pour ajouter des lignes avec NaN pour les listes vides
    #     additional_rows = []
    #     for item in json_data:
            
    #         if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
    #             bnode_counter_subject +=1
    #             bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

    #             subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
    #             object_empty=bnode_subject
                
    #             additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"{object_empty}" })
    #             #additional_rows.append({ 'id': item['id'], 'subject': rml_rule['subject_map_value'], 'predicate':"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt>" })

    #     # Ajouter les nouvelles lignes au dataframe
    #     if additional_rows:
    #         df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)

    #     print("new df", df)
    

    #Partie 2 : remplissage du dataframe df2 comme une collection (List) ou un container (Alt, Bag ou Seq)
    #Blank nodes subject (Réinitialisation du compteur au départ)

    if rml_rule['gatherAs'] in [RDF_LIST]:
        # FIN PARTIE 1

        print("df", df)

    
        json_data=load_json(rml_rule)
        check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
        references = get_references_in_template(rml_rule['subject_map_value'])


        #Cette partie complète convenablement le 1er dataframe lorsque les listes vides sont autorisées     
        if (check_list==True and rml_rule['allowEmptyListAndContainer']=="true"): #TODO listes vides par défaut pour plusieurs rml:reference
            print("liste vide !!!!")
            # Traiter le dataframe pour ajouter des lignes avec NaN pour les listes vides
            additional_rows = []
            for item in json_data:
                
                if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
                    bnode_counter_subject +=1
                    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

                    subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                    object_empty=bnode_subject
                    
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"})
                    #additional_rows.append({ 'id': item['id'], 'subject': rml_rule['subject_map_value'], 'predicate':"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt>" })

            # Ajouter les nouvelles lignes au dataframe
            if additional_rows:
                df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)

            #print("new df", df)


            

        #Initialisation des blank nodes qui seront subject pour les listes 
        bnode_counter_subject=1
        bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
        subject_value=bnode_subject

        bnode_counter=1

        # Boucle parcourant le dataframe contenant les données 
        for i in range(len(results_df[rml_rule['gather_reference']])):
            
            if i < len(results_df) - 1:
                #si la value actuelle et celle d'après ont le même id --> début ou suite d'une liste --> Le prochain subject sera une blank node
                if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                    bnode = f"_:bnode{bnode_counter}"

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                    }, ignore_index=True)

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                        'object': f"{bnode}"
                        }, ignore_index=True)
                    subject_value = bnode                        
                    #bnode = f"_:bnode{bnode_counter}"
                    bnode_counter += 1        

                #si la value actuelle et celle d'après n'ont PAS le même id --> fin d'une liste --> Le prochain subject sera une blank node subject DIFFERENTE
                elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: #or results_df.loc[i+1, 'id'] == None:
                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                            }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                            'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
                            }, ignore_index=True)
                        
                        bnode_counter_subject +=1
                        bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

                        subject_value=bnode_subject
                        # bnode_counter += 1      
                        # bnode = f"_:bnode{bnode_counter}"

            #la value actuelle est la dernière du dataframe --> fin de la liste
            else:
                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
                        'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
                        }, ignore_index=True)


        # print("df", df)
        # print("df2", df2)
        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)

        # print("df_final", df_final)
    
    
    elif rml_rule['gatherAs'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:
        json_data=load_json(rml_rule)
        check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
        references = get_references_in_template(rml_rule['subject_map_value'])


        #Cette partie complète convenablement le 1er dataframe lorsque les listes vides sont autorisées     
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
            # Traiter le dataframe pour ajouter des lignes avec NaN pour les listes vides
            additional_rows = []
            for item in json_data:
                
                if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
                    bnode_counter_subject +=1
                    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

                    subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                    object_empty=bnode_subject
                    
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"{object_empty}" })
                    #additional_rows.append({ 'id': item['id'], 'subject': rml_rule['subject_map_value'], 'predicate':"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt>" })

            # Ajouter les nouvelles lignes au dataframe
            if additional_rows:
                df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)

            print("new df", df)




        count=1

        #Initialisation des blank nodes qui seront subject pour les listes 
        bnode_counter_subject=1
        bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
        subject_value=bnode_subject

        #print("REFERENCES[0]", references[0]) --> id 
        #print("rml_rule['gather_reference']", rml_rule['gather_reference']) --> vala

        print("results_df",results_df )
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
            # Identification des lignes manquantes entre df et results_df
            merged_df = df.merge(results_df, on=[references[0], rml_rule['gather_reference']], how='left', indicator=True)
            missing_rows = merged_df[merged_df['_merge'] == 'left_only'][[references[0], rml_rule['gather_reference']]]

            # Ajout des lignes manquantes à results_df
            results_df = results_df._append(missing_rows, ignore_index=True)
        
        print("results_df",results_df )


 
        #Détermine quel container on a 
        if rml_rule['gatherAs'] == RDF_BAG:
            collection = 'Bag'
        elif rml_rule['gatherAs'] == RDF_SEQ:
            collection = 'Seq'
        elif rml_rule['gatherAs'] == RDF_ALT:
            collection = 'Alt'


        # Boucle parcourant le dataframe contenant les données 
        for i in range(len(results_df[rml_rule['gather_reference']])):
            
            if i < len(results_df) - 1:
                #si la value actuelle et celle d'après ont le même id --> début ou suite d'un container --> Le prochain subject sera le même que actuellemment
                if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                    # bnode = f"_:bnode{bnode_counter}"

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                    }, ignore_index=True)

                    count+=1


                #si la value actuelle et celle d'après n'ont PAS le même id --> fin du container --> Le prochain subject sera une blank node DIFFERENTE
                elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: #or results_df.loc[i+1, 'id'] == None:
                    
                    
                    #Container vide
                    if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                        }, ignore_index=True)

                    #Container non vide
                    else:
                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                            'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                            }, ignore_index=True)

                    count=1    

                    bnode_counter_subject +=1
                    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
                    subject_value=bnode_subject
                        # bnode_counter += 1      
                        # bnode = f"_:bnode{bnode_counter}"

            #la value actuelle est la dernière du dataframe --> fin de la liste
            else:
                #Container vide
                if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                    }, ignore_index=True)

                #Container non vide
                else:
                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                        }, ignore_index=True)
                    
                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                         }, ignore_index=True)
        
        # print("df", df)
        # print("df2", df2)
        
        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)

    else:
        df_final = df


    return df_final



def _merge_data(data, parent_data, rml_rule, join_condition):
    parent_data = parent_data.add_prefix('parent_')
    child_join_references, parent_join_references = get_references_in_join_condition(rml_rule, join_condition)
    parent_join_references = ['parent_' + reference for reference in parent_join_references]

    # if there is only one join condition use join, otherwise use merge
    if len(child_join_references) == 1:
        data = data.set_index(child_join_references, drop=False)
        parent_data = parent_data.set_index(parent_join_references, drop=False)
        return data.join(parent_data, how='inner')
    else:
        return data.merge(parent_data, how='inner', left_on=child_join_references, right_on=parent_join_references)


def _materialize_rml_rule(rml_rule, rml_df, fnml_df, config, data=None, parent_join_references=set(), nest_level=0,
                          python_source=None):

    
    references = set(_get_references_in_rml_rule(rml_rule, rml_df, fnml_df))
    #print("references", references)

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, 'subject_join_conditions')
    references_object_join, parent_references_object_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')
    references.update(parent_join_references)

    # print("rml_rule['subject_map_type']", rml_rule['subject_map_type'])
    # print("rml_rule['object_map_type']", rml_rule['object_map_type'])
    # print("rml_rule['object_termtype']", rml_rule['object_termtype'])
    # print("rml_rule['object_map_value']", rml_rule['object_map_value'])

    # print("rml_rule['gather']", rml_rule['gather'])


    # handle the case in which all term maps are constant-valued
    if rml_rule['subject_map_type'] == RML_CONSTANT and rml_rule['predicate_map_type'] == RML_CONSTANT and rml_rule['object_map_type'] == RML_CONSTANT and rml_rule['graph_map_type'] == RML_CONSTANT:
        # create a dataframe with 1 row
        data = pd.DataFrame({'placeholder': ['placeholder']})
        data = _materialize_rml_rule_terms(data, rml_rule, fnml_df, config)

    elif rml_rule['subject_map_type'] == RML_QUOTED_TRIPLES_MAP or rml_rule['object_map_type'] == RML_QUOTED_TRIPLES_MAP:

        if data is None:
            data = _get_data(config, rml_rule, references, python_source)

        if rml_rule['subject_map_type'] == RML_QUOTED_TRIPLES_MAP:
            if pd.notna(rml_rule['subject_join_conditions']):
                references.update(references_subject_join)
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
                parent_data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config,
                                                    parent_join_references=parent_references_subject_join,
                                                    nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, rml_rule, 'subject_join_conditions')
                data['subject'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
                data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config, data=data,
                                             nest_level=nest_level + 1)
                data['subject'] = '<< ' + data['triple'] + ' >>'
            data['keep_subject' + str(nest_level)] = data['subject']
        if rml_rule['object_map_type'] == RML_QUOTED_TRIPLES_MAP:
            if pd.notna(rml_rule['object_join_conditions']):
                references.update(references_object_join)
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
                parent_data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config,
                                                    parent_join_references=parent_references_object_join,
                                                    nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, rml_rule, 'object_join_conditions')
                data['object'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
                data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config, data=data,
                                             nest_level=nest_level + 1)
                data['object'] = '<< ' + data['triple'] + ' >>'
            if rml_rule['subject_map_type'] == RML_QUOTED_TRIPLES_MAP:
                data['subject'] = data['keep_subject' + str(nest_level)]

        data = _materialize_rml_rule_terms(data, rml_rule, fnml_df, config)

    # elif pd.notna(rml_rule['object_parent_triples_map']):
    elif rml_rule['object_map_type'] == RML_PARENT_TRIPLES_MAP:

        references.update(references_object_join)
        # parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_parent_triples_map'])
        parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
        parent_references = set(
            _get_references_in_rml_rule(parent_triples_map_rule, rml_df, fnml_df, only_subject_map=True))

        # add references used in the join condition
        references, parent_references = _add_references_in_join_condition(rml_rule, references, parent_references)

        if data is None:
            data = _get_data(config, rml_rule, references, python_source)

        parent_data = _get_data(config, parent_triples_map_rule, parent_references, python_source)
        merged_data = _merge_data(data, parent_data, rml_rule, 'object_join_conditions')

        rml_rule['object_map_type'] = parent_triples_map_rule['subject_map_type']
        rml_rule['object_map_value'] = parent_triples_map_rule['subject_map_value']

        data = _materialize_rml_rule_terms(merged_data, rml_rule, fnml_df, config, columns_alias='parent_')

    
    #Named collection or container
    elif rml_rule['object_map_type'] == RML_TEMPLATE and pd.notna(rml_rule['gather']):
        print("named list")
        if data is None:
            data = _get_data(config, rml_rule, references, python_source)
        
        print("data2", data)

        data = _materialize_rml_rule_terms_named_cc(data, rml_rule, fnml_df, config)
        #print("data ", data)

    #Unnamed collection or container
    elif pd.isna(rml_rule['object_map_type']) and pd.notna(rml_rule['gather']):
        print ("unnamed list")


        print("rml_rule['mapping_partition']", rml_rule['mapping_partition'])
        print("rml_rule['object_map_type']", rml_rule['object_map_type'])
        print("rml_rule['object_map_value']", rml_rule['object_map_value'])
        print("rml_rule['object_termtype']", rml_rule['object_termtype'])
        print("rml_rule['gather_map_object']", rml_rule['gather_map_object'])

        print("rml_rule['gather_reference']", rml_rule['gather_reference'])
        print("rml_rule['strategy']", rml_rule['strategy'])
        print("rml_rule['gather_node']", rml_rule['gather_node'])
        print("rml_rule['gather']", rml_rule['gather'])
        print("rml_rule['gather_referenceS']", rml_rule['gather_references'])
        print("rml_rule['iterator']", rml_rule['iterator'])

        



        if data is None:
            data = _get_data(config, rml_rule, references, python_source)
        
        data = _materialize_rml_rule_terms_unnamed_cc(data, rml_rule, fnml_df, config)

    else:

        if data is None:
            data = _get_data(config, rml_rule, references, python_source)

        data = _materialize_rml_rule_terms(data, rml_rule, fnml_df, config)

    # TODO: this is slow reduce the number of vectorized operations
    data['triple'] = data['subject'] + ' ' + data['predicate'] + ' ' + data['object']

    if nest_level == 0 and config.get_output_format() == NQUADS:
        if rml_rule['graph_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE] and rml_rule['graph_map_value'] != RML_DEFAULT_GRAPH:
            data = _materialize_template(data, rml_rule['graph_map_value'], rml_rule['graph_map_type'], config, 'graph', termtype=RML_IRI)
        elif rml_rule['graph_map_type'] == RML_EXECUTION:
            data = _materialize_fnml_execution(data, rml_rule['graph_map_value'], fnml_df, config, 'graph', termtype=RML_IRI)
        else:
            data['graph'] = ''
        data['triple'] = data['triple'] + ' ' + data['graph']

    data = data.drop(columns=['subject', 'predicate', 'object'], errors='ignore')
    
    return data


def _materialize_mapping_group_to_set(mapping_group_df, rml_df, fnml_df, config, python_source=None):
    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        data = _materialize_rml_rule(rml_rule, rml_df, fnml_df, config, python_source=python_source)
        triples.update(set(data['triple']))

    return triples


def _materialize_mapping_group_to_file(mapping_group_df, rml_df, fnml_df, config):
    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        start_time = time.time()
        data = _materialize_rml_rule(rml_rule, rml_df, fnml_df, config)
        triples.update(set(data['triple']))

        logging.debug(f"{len(triples)} triples generated for mapping rule `{rml_rule['triples_map_id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_file(triples, config, mapping_group_df.iloc[0]['mapping_partition'])

    return len(triples)


def _materialize_mapping_group_to_kafka(mapping_group_df, rml_df, fnml_df, config, python_source=None):
    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        start_time = time.time()
        data = _materialize_rml_rule(rml_rule, rml_df, fnml_df, config, python_source=python_source)
        triples.update(set(data['triple']))

        logging.debug(f"{len(triples)} triples generated for mapping rule `{rml_rule['triples_map_id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_kafka(triples, config)

    return len(triples)
