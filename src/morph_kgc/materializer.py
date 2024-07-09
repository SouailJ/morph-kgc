__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

# import uuid
# import numpy as np
# from rdflib import URIRef, Literal, RDF

from falcon.uri import encode_value
from urllib.parse import quote

from .utils import *
from .constants import *
from .data_source.relational_db import get_sql_data
from .data_source.property_graph_db import get_pg_data
from .data_source.data_file import get_file_data

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
    
    # Added for RML-CC : If there is a gather map in the mapping, the rml:reference is added to the list named "references"
    if pd.notna(rml_rule['gather']):
        # rml_rule['gather_references'] contains the rml_reference (one or more) in rml_gather, separated by a comma          
        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        for ref in refs:
            references.append(ref)


    # term maps with join conditions (referencing and quoted)
    positions = ['subject'] if only_subject_map else ['subject', 'object']
    for position in positions:
        if rml_rule[f'{position}_map_type'] == RML_QUOTED_TRIPLES_MAP and pd.isna(rml_rule[f'{position}_join_conditions']):
            parent_rml_rule = get_rml_rule(rml_df, rml_rule[f'{position}_map_value'])
            references.extend(_get_references_in_rml_rule(parent_rml_rule, rml_df, fnml_df))

        references_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, f'{position}_join_conditions')
        references.extend(references_join)

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


# Added for RML-CC : this function concatenates 2 dataframes
# The first one (df) represent a triple --> The subject is the subject_map_value, the predicate is the predicate_map_value, and the object is the object_map_value (call of the function _materialize_rml_rule_terms)
# The second dataframe (df2) will contain a list or a container (Bag, Alt or Seq)

def _materialize_rml_rule_terms_named_cc(results_df, rml_rule, config, columns_alias=''):
    
    # refs contains the rml:reference (one or more) in a rml:gather
    refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]

    #If there are several rml:reference in a rml:gather, we modify results_df to have all the values in the same column of the dataframe.
    if len(refs) > 1:
        references = get_references_in_template(rml_rule['subject_map_value'])

        # Merge the columns specified in refs into a single 'val' column
        melted_df = pd.melt(results_df, id_vars=[references[0]], value_vars=refs, var_name='variable', value_name='val')

        melted_df = melted_df.drop(columns=['variable'])
        melted_df = melted_df.drop_duplicates().reset_index(drop=True)
        melted_df = melted_df.sort_values(by=[references[0]] + ['val']).reset_index(drop=True)

        results_df=melted_df
        # This line allows us to assume that we have a single rml:reference, when in fact there are several.        
        rml_rule['gather_reference']='val'


    # Create both dataframe which will contain the triples resulting of the mapping
    df=pd.DataFrame(results_df)
    df2=pd.DataFrame()

    #Fill the first dataframe 
    df=_materialize_rml_rule_terms(df, rml_rule, None, config, columns_alias='')

    # Named list
    if rml_rule['gatherAs'] in [RDF_LIST]:

        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        refs = list(set(refs))
        json_data=load_json(rml_rule)

        
        #If there are several rml:references in one rml:gather
        if len(refs)>1:
            df, results_df = manage_several_templates_named_cc(json_data, refs, rml_rule, df, results_df)


        #Case with only one rml:reference
        else:
            df=manage_empty_named_cc(json_data, rml_rule, df)
            
        # Fill dataframe as a named collection    
        df2=fill_dataframe_as_named_collection(results_df, rml_rule, df2)

        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)

    
    # Named container (Bag, Alt ou Seq)
    elif rml_rule['gatherAs'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:

        
        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        refs = list(set(refs))
        json_data=load_json(rml_rule)

        #If there are several rml:references in one rml:gather
        if len(refs)>1:
            df, results_df = manage_several_templates_named_cc(json_data, refs, rml_rule, df, results_df)


        else:
            df=manage_empty_named_cc(json_data, rml_rule, df)
            
        
        # At this point, the df dataframe is well filled for a named container (one or several rml:reference, empty lists or not)
        #Then, the second part will allow us to fill the df2 dataframe to contain a list        
        
        check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
        references = get_references_in_template(rml_rule['object_map_value'])

        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
            # Identifying missing lines between df and results_df           
            merged_df = df.merge(results_df, on=[references[0], rml_rule['gather_reference']], how='left', indicator=True)
            missing_rows = merged_df[merged_df['_merge'] == 'left_only'][[references[0], rml_rule['gather_reference']]]

            # Adding missing rows to results_df
            results_df = results_df._append(missing_rows, ignore_index=True)


        #Fill df2 as a named container
        df2=fill_dataframe_as_named_container(results_df, rml_rule, df2, references)
                    
        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)

    else:
        df_final = df

    return df_final
   
# Added for RML-CC : this function concatenates 2 dataframes
# The first one (df) represent a triple --> The subject is the subject_map_value, the predicate is the predicate_map_value, and the object is the object_map_value (call of the function _materialize_rml_rule_terms)
# The second dataframe (df2) will contain an unnamed list or container (Bag, Alt or Seq)
def _materialize_rml_rule_terms_unnamed_cc(results_df, rml_rule, config, columns_alias=''):

    # refs contains the rml:reference (one or more) in a rml:gather    
    refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]

    #If there are several rml:reference in a rml:gather, we modify results_df to have all the values in the same column of the dataframe.
    if len(refs) > 1:
        references = get_references_in_template(rml_rule['subject_map_value'])
    
        # Merge the columns specified in refs into a single 'val' column
        melted_df = pd.melt(results_df, id_vars=[references[0]], value_vars=refs, var_name='variable', value_name='val')

        melted_df = melted_df.drop(columns=['variable'])
        melted_df = melted_df.drop_duplicates().reset_index(drop=True)
        melted_df = melted_df.sort_values(by=[references[0]] + ['val']).reset_index(drop=True)

        results_df=melted_df
        # This line allows us to assume that we have a single rml:reference, when in fact there are several.        
        rml_rule['gather_reference']='val'

    # Create both dataframe which will contain the triples resulting of the mapping
    df=pd.DataFrame(results_df)
    df2=pd.DataFrame()
    
    #Fill the first dataframe 
    df=_materialize_rml_rule_terms(df, rml_rule, None, config, columns_alias='')
    
    references = get_references_in_template(rml_rule['subject_map_value'])

    #Blank nodes which are subjects, incremented using a counter
    bnode_counter_subject=1
    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
    
    # Loop through the dataframe containing the data 
    # This loop has as goal to add in the dataframe a blank node for each triple before the list/container. These blank nodes will be then the subject of lists/containers
    for i in range(len(results_df[rml_rule['gather_reference']])):
        if i < len(results_df) - 1:

            #if the current value and the next value have the same key in the json file --> start or continue a list/container
            # --> The object will be a blank node (the same as above if you have already passed through this if condition).
            if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                df.loc[i, 'object'] = bnode_subject
            
            #if the current value and the next value do NOT have the same key in the json file --> end of a list/container --> the next object will be ANOTHER blank node (incrementation)
            elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]:
                df.loc[i, 'object'] = bnode_subject
                bnode_counter_subject +=1
                bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

        #the current value is the last in the dataframe --> end of list/container
        else:
            df.loc[i, 'object'] = bnode_subject


    if rml_rule['gatherAs'] in [RDF_LIST]:

        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        refs = list(set(refs))
        json_data=load_json(rml_rule)

        #If there are several rml:references in one rml:gather
        if len(refs)>1:
            df, results_df = manage_several_templates_unnamed_cc(json_data, refs, rml_rule, df, results_df, bnode_counter_subject)

        else:
            df= manage_empty_unnamed_cc(json_data, rml_rule, df, bnode_counter_subject)
            
        #Fill df2 as an unnamed collection
        df2=fill_dataframe_as_unnamed_collection(results_df, rml_rule, df2, references)


        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)    
    
    elif rml_rule['gatherAs'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:

        refs = [ref.strip() for ref in rml_rule['gather_references'].split(',')]
        refs = list(set(refs))
        json_data=load_json(rml_rule)
        
        #If there are several rml:references in one rml:gather
        if len(refs)>1:
            df, results_df = manage_several_templates_unnamed_cc(json_data, refs, rml_rule, df, results_df, bnode_counter_subject)


        else:
            df= manage_empty_unnamed_cc(json_data, rml_rule, df, bnode_counter_subject)

            

        # At this point, the df dataframe is well filled for a named container (one or several rml:reference, empty lists or not)
        #Then, the second part will allow us to fill the df2 dataframe to contain a container 
        
        check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
            # Identifying missing lines between df and results_df           
            merged_df = df.merge(results_df, on=[references[0], rml_rule['gather_reference']], how='left', indicator=True)
            missing_rows = merged_df[merged_df['_merge'] == 'left_only'][[references[0], rml_rule['gather_reference']]]

            # Adding missing rows to results_df
            results_df = results_df._append(missing_rows, ignore_index=True)
        

        #Fill df2 as an unnamed container
        df2=fill_dataframe_as_unnamed_container(results_df, rml_rule, df2, references)

        # Combine df and df2
        df_final = pd.concat([df, df2], axis=0)
        df_final = df_final.reset_index(drop=True)

    else:
        df_final = df


    return df_final

# Added for RML-CC : this function generate an unnamed list or container in the subject map 
def _materialize_rml_rule_terms_unnamed_cc_SM(results_df, rml_rule):
    
    json_data=load_json(rml_rule)

    # The df dataframe only contains data to fill results_df. The df2 dataframe contains a list or a container
    df = pd.DataFrame(json_data)
    df2=pd.DataFrame()

    # Reorganisation of the DataFrame
    df = df[df[df.columns[1]].map(lambda x: len(x) > 0)]
    results_df = df.explode(df.columns[1]).reset_index(drop=True)

    #Initialisation of blank nodes that will be used as subjects for lists and containers 
    bnode_counter_subject=1
    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
    subject_value=bnode_subject

    #Condition whether there is not predicate object map  
    if pd.notna(rml_rule['predicate_map_value']) and pd.notna(rml_rule['object_map_value']):    
        df2 = df2._append({
            'subject': f"{subject_value}",
            'predicate': f"<{rml_rule['predicate_map_value']}>",
            'object': f"<{rml_rule['object_map_value']}>"
        }, ignore_index=True)


    bnode_counter=1

    if rml_rule['gatherAs_subject'] in [RDF_LIST]:
        
        # Loop through the dataframe containing the data 
        for i in range(len(results_df[rml_rule['gather_reference']])):
            
            if i < len(results_df) - 1:
                #if the current value and the next value have the same key in the json file --> start or continue a list --> The next subject will be a blank node
                if (results_df.loc[i, results_df.columns[0]])==(results_df.loc[i+1, results_df.columns[0]]):
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
                    bnode_counter += 1        

                #if the current value and the next value do NOT have the same key  --> end of list --> next subject will be another blank node
                elif results_df.loc[i, results_df.columns[0]]!= results_df.loc[i+1, results_df.columns[0]]: #or results_df.loc[i+1, 'id'] == None:
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

                        #Condition whether there is not predicate object map  
                        if pd.notna(rml_rule['predicate_map_value']) and pd.notna(rml_rule['object_map_value']):
                            df2 = df2._append({
                                'subject': f"{subject_value}",
                                'predicate': f"<{rml_rule['predicate_map_value']}>",
                                'object': f"<{rml_rule['object_map_value']}>"
                            }, ignore_index=True)

            #the current value is the last in the dataframe --> end of list or start of a new list of one element            
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
    
    
    elif rml_rule['gatherAs_subject'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:
        #Determine which container we have
        if rml_rule['gatherAs_subject'] == RDF_BAG:
            collection = 'Bag'
        elif rml_rule['gatherAs_subject'] == RDF_SEQ:
            collection = 'Seq'
        elif rml_rule['gatherAs_subject'] == RDF_ALT:
            collection = 'Alt'

        # This counter allow us to "name" the predicates for the container
        count=1

        # Loop through the dataframe containing the data
        for i in range(len(results_df[rml_rule['gather_reference']])):
            if i < len(results_df) - 1:
                #if the current value and the next value have the same key --> start or continuation of a container --> The next subject will be the same as the current subject (Blank node)
                if (results_df.loc[i, results_df.columns[0]])==(results_df.loc[i+1, results_df.columns[0]]):

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                    }, ignore_index=True)

                    count+=1


                #if the current value and the next value do NOT have the same key --> end of container --> next subject will be a different blank node                
                elif results_df.loc[i, results_df.columns[0]]!= results_df.loc[i+1, results_df.columns[0]]: #or results_df.loc[i+1, 'id'] == None:
                    
                    
                    #Empty container
                    if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                        }, ignore_index=True)

                    #Non-empty container
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

                    #Definition of the new subject for the next triple  
                    bnode_counter_subject +=1
                    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
                    subject_value=bnode_subject

                    #Condition whether there is not predicate object map  
                    if pd.notna(rml_rule['predicate_map_value']) and pd.notna(rml_rule['object_map_value']):
                        df2 = df2._append({
                            'subject': f"{subject_value}",
                            'predicate': f"<{rml_rule['predicate_map_value']}>",
                            'object': f"<{rml_rule['object_map_value']}>"
                        }, ignore_index=True)


            #the current value is the last in the dataframe --> end of container            
            else:
                #Empty container
                if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                    }, ignore_index=True)

                #Non-empty container
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
        
    return df2

# Added for RML-CC : this function generate an named list or container in the subject map 
def _materialize_rml_rule_terms_named_cc_SM(results_df, rml_rule):

    json_data=load_json(rml_rule)

    # The df dataframe only contains data to fill results_df. The df2 dataframe contains a list or a container
    df = pd.DataFrame(json_data)
    df2=pd.DataFrame()

    # Reorganisation of the DataFrame
    df = df[df[df.columns[1]].map(lambda x: len(x) > 0)]
    results_df = df.explode(df.columns[1]).reset_index(drop=True)

    #We define the subject value (at the start)    
    subject_value = rml_rule['subject_map_value'].replace(f'{{{results_df.columns[0]}}}', str(results_df.loc[0, results_df.columns[0]]))

    
    #Condition whether there is not predicate object map  
    if pd.notna(rml_rule['predicate_map_value']) and pd.notna(rml_rule['object_map_value']):
        df2 = df2._append({
            'subject': f"<{subject_value}>",
            'predicate': f"<{rml_rule['predicate_map_value']}>",
            'object': f"<{rml_rule['object_map_value']}>"
        }, ignore_index=True)



    if rml_rule['gatherAs_subject'] in [RDF_LIST]: 

        #Blank nodes incremented using a counter
        bnode_counter=1
        bnode = f"_:bnode{bnode_counter}"

        # Loop through the dataframe containing the data 
        for i in range(len(results_df[rml_rule['gather_reference']])):
            
            if i < len(results_df) - 1:
                #if the current value and the next value have the same key in the json file --> start or continue a list --> The next subject will be a blank node
                if (results_df.loc[i, results_df.columns[0]])==(results_df.loc[i+1, results_df.columns[0]]):

                    # Continue a list 
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
                        bnode_counter += 1        
                    
                    # Start of the list
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
    
                #if the current value and the next value do NOT have the same key  --> end of list --> next subject will be an IRI 
                elif results_df.loc[i, results_df.columns[0]]!= results_df.loc[i+1, results_df.columns[0]]: #or results_df.loc[i+1, 'id'] == None:
                    #end of the list 
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
                        
                        subject_value = rml_rule['subject_map_value'].replace(f'{{{results_df.columns[0]}}}', str(results_df.loc[i+1, results_df.columns[0]]))
                        bnode_counter += 1      
                        bnode = f"_:bnode{bnode_counter}"

                        #Condition whether there is not predicate object map  
                        if pd.notna(rml_rule['predicate_map_value']) and pd.notna(rml_rule['object_map_value']):
                            df2 = df2._append({
                                'subject': f"<{subject_value}>",
                                'predicate': f"<{rml_rule['predicate_map_value']}>",
                                'object': f"<{rml_rule['object_map_value']}>"
                            }, ignore_index=True)

                    # start and end of the list     
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
                        subject_value = rml_rule['object_map_value'].replace(f'{{{results_df.columns[0]}}}', str(results_df.loc[i+1, results_df.columns[0]]))
                        bnode_counter += 1      
                        bnode = f"_:bnode{bnode_counter}"

            #the current value is the last in the dataframe --> end of list or start of a new list of one element            
            else:
                # end of the last list
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
                
                # start of a new list of one element
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
                      
    elif rml_rule['gatherAs_subject'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:
        #Determine which container we have
        if rml_rule['gatherAs_subject'] == RDF_BAG:
            collection = 'Bag'
        elif rml_rule['gatherAs_subject'] == RDF_SEQ:
            collection = 'Seq'
        elif rml_rule['gatherAs_subject'] == RDF_ALT:
            collection = 'Alt'

        # This counter allow us to "name" the predicates for the container
        count=1

        # Loop through the dataframe containing the data
        for i in range(len(results_df[rml_rule['gather_reference']])):

            if i < len(results_df) - 1:
                #if the current value and the next value have the same key --> start or continuation of a container --> The next subject will be the same as the current subject (IRI)
                if (results_df.loc[i, results_df.columns[0]])==(results_df.loc[i+1, results_df.columns[0]]):

                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                        'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                    }, ignore_index=True)



                    count+=1

                #if the current value and the next value do NOT have the same key --> end of container --> next subject will be a different IRI                
                elif results_df.loc[i, results_df.columns[0]]!= results_df.loc[i+1, results_df.columns[0]]: #or results_df.loc[i+1, 'id'] == None:

                    #Empty container
                    if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                        }, ignore_index=True)

                    #Non-empty container
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

                    #Definition of the new subject for the next triple  
                    subject_value = rml_rule['subject_map_value'].replace(f'{{{results_df.columns[0]}}}', str(results_df.loc[i+1, results_df.columns[0]]))

                    #Condition whether there is not predicate object map  
                    if pd.notna(rml_rule['predicate_map_value']) and pd.notna(rml_rule['object_map_value']):
                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': f"<{rml_rule['predicate_map_value']}>",
                            'object': f"<{rml_rule['object_map_value']}>"
                        }, ignore_index=True)



            #the current value is the last in the dataframe --> end of container            
            else:
                #Empty container
                if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):
                        df2 = df2._append({
                            'subject': f"<{subject_value}>",
                            'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                        }, ignore_index=True)
                
                #Non-empty container
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

    return df2



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

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, 'subject_join_conditions')
    references_object_join, parent_references_object_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')
    references.update(parent_join_references)

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

    
    #Named collection or container in the objectMap
    elif rml_rule['object_map_type'] == RML_TEMPLATE and pd.notna(rml_rule['gather']):
        if data is None:
            data = _get_data(config, rml_rule, references, python_source)
        
        data = _materialize_rml_rule_terms_named_cc(data, rml_rule, config)

    #Unnamed collection or container in the objectMap
    elif pd.isna(rml_rule['object_map_type']) and pd.notna(rml_rule['predicate_map_type']) and pd.notna(rml_rule['gather']):
        if data is None:
            data = _get_data(config, rml_rule, references, python_source)
        
        data = _materialize_rml_rule_terms_unnamed_cc(data, rml_rule, config)
    
    #Unnamed collection or container in the subjectMap
    elif rml_rule['subject_map_type']==RML_GATHER:
        if data is None:
            data = _get_data(config, rml_rule, references, python_source)
        
        data = _materialize_rml_rule_terms_unnamed_cc_SM(data, rml_rule)

    #Named collection or container in the subjectMap
    elif rml_rule['subject_map_type'] == RML_TEMPLATE and pd.notna(rml_rule['gather_subject']): 
        if data is None:
            data = _get_data(config, rml_rule, references, python_source)
        
        data = _materialize_rml_rule_terms_named_cc_SM(data, rml_rule)

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


def manage_several_templates_named_cc(json_data, refs, rml_rule, df, results_df):

    for ref in refs:
        
        check_list=check_for_empty_lists(json_data, ref)
        references = get_references_in_template(rml_rule['subject_map_value'])
        
        #if there is an empty list in the data file
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="false":

            #we add to df the values "linked" to the empty container (same item but not the same key in the json file), because the df dataframe is not vell filled by default
            additional_rows = []
            for item in json_data:
                for index, (key, value) in enumerate(item.items()):
                    if index == 0:
                        continue
                    for val in value:

                        if ref in item and isinstance(item[ref], list) and len(item[ref]) == 0:
                            subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                            object_empty=rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                    
                            additional_rows.append({ references[0]: item[references[0]], 'val': val, 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"<{object_empty}>"})

            # Add the additional rows to the dataframe df (which contains the triples before the container)
            if additional_rows:
                df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)


            #we add to results_df the values "linked" to the empty container (same item but not the same key in the json file), because the results_df dataframe is not well filled by default
            additional_rows2 = []
            for item in json_data:
                for index, (key, value) in enumerate(item.items()):
                    if index == 0:
                        continue
                    for val in value:

                        if ref in item and isinstance(item[ref], list) and len(item[ref]) == 0:
                            subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                            object_empty=rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                    
                            additional_rows2.append({ references[0]: item[references[0]], 'val': val})

            if additional_rows2:
                results_df = pd.concat([results_df, pd.DataFrame(additional_rows2)], ignore_index=True)

    return df, results_df

def manage_several_templates_unnamed_cc(json_data, refs, rml_rule, df, results_df, bnode_counter_subject):

    for ref in refs:
        check_list=check_for_empty_lists(json_data, ref)
        references = get_references_in_template(rml_rule['subject_map_value'])

        #if there is an empty list in the data file
        if check_list==True and rml_rule['allowEmptyListAndContainer']=="false":

            #we add to df the values "linked" to the empty container (same item but not the same key in the json file), because the df dataframe is not vell filled by default
            additional_rows = []
            subject_empty=None

            for item in json_data:
                for index, (key, value) in enumerate(item.items()):
                    if index == 0:
                        continue
                    for val in value:
                        
                        if ref in item and isinstance(item[ref], list) and len(item[ref]) == 0:

                            if subject_empty!=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]])):
                                bnode_counter_subject +=1
                                bnode_subject = f"_:bnode_subject{bnode_counter_subject}"


                                subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                                object_empty=bnode_subject

                            additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"{object_empty}"})
                                        
            # Add the additional rows to the dataframe df (which contains the triples before the container)
            if additional_rows:
                df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)


            #we add to results_df the values "linked" to the empty container (same item but not the same key in the json file), because the results_df dataframe is not well filled by default
            additional_rows2 = []
            for item in json_data:
                for index, (key, value) in enumerate(item.items()):
                    if index == 0:
                        continue
                    for val in value:
                        
                        if ref in item and isinstance(item[ref], list) and len(item[ref]) == 0:            

                            additional_rows2.append({ references[0]: item[references[0]], 'val': val})

            if additional_rows2:
                results_df = pd.concat([results_df, pd.DataFrame(additional_rows2)], ignore_index=True)
    return df, results_df

def manage_empty_named_cc(json_data, rml_rule, df):
    check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
    references = get_references_in_template(rml_rule['subject_map_value'])   

    #Manage the df dataframe to allow empty lists
    if check_list==True and rml_rule['allowEmptyListAndContainer']=="true":
        # This part will add a row to the df dataframe for each empty list
        additional_rows = []
        for item in json_data:
            subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
            object_empty=rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
            if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
                if rml_rule['gatherAs'] in [RDF_LIST]:
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"})

                elif rml_rule['gatherAs'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"<{object_empty}>" })

        if additional_rows:
            df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)
    return df


def manage_empty_unnamed_cc(json_data, rml_rule, df, bnode_counter_subject):
    check_list=check_for_empty_lists(json_data, rml_rule['gather_reference'])
    references = get_references_in_template(rml_rule['subject_map_value'])

    #Manage the df dataframe to allow empty lists
    if (check_list==True and rml_rule['allowEmptyListAndContainer']=="true"): 
        # This part will add a row to the df dataframe for each empty list
        additional_rows = []
        for item in json_data:
            
            if rml_rule['gather_reference'] in item and isinstance(item[rml_rule['gather_reference']], list) and len(item[rml_rule['gather_reference']]) == 0:
                bnode_counter_subject +=1
                bnode_subject = f"_:bnode_subject{bnode_counter_subject}"

                subject_empty=rml_rule['subject_map_value'].replace(f'{{{references[0]}}}', str(item[references[0]]))
                object_empty=bnode_subject

                if rml_rule['gatherAs'] in [RDF_LIST]:
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"})

                elif rml_rule['gatherAs'] in [RDF_ALT, RDF_BAG, RDF_SEQ]:
                    additional_rows.append({ references[0]: item[references[0]], 'subject': f"<{subject_empty}>", 'reference_results':str(item[references[0]]), 'predicate':f"<{rml_rule['predicate_map_value']}>", 'object': f"{object_empty}" })

        if additional_rows:
            df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)
    return df


def fill_dataframe_as_named_collection(results_df, rml_rule, df2):
    
    references = get_references_in_template(rml_rule['object_map_value'])        

    #We define the value of the subject (at the start)
    subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[0, references[0]]))

    #Blank nodes incremented using a counter
    bnode_counter=1
    bnode = f"_:bnode{bnode_counter}"


    # Loop through the dataframe containing the data 
    for i in range(len(results_df[rml_rule['gather_reference']])):
        
        if i < len(results_df) - 1:
            #if the current value and the next value have the same key in the json file --> start or continue a list --> The next subject will be a blank node
            if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):
                # Continue a list 
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

                # Start of the list
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

            #if the current value and the next value do NOT have the same key  --> end of list --> next subject will be an IRI 
            elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: 
                #end of the list 
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

                # start and end of the list     
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

        #the current value is the last in the dataframe --> end of list or start of a new list of one element            
        else:
            # end of the last list
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
            
            # start of a new list of one element
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
    return df2



def fill_dataframe_as_named_container(results_df, rml_rule, df2, references):

    #Determine which container we have
    if rml_rule['gatherAs'] == RDF_BAG:
        collection = 'Bag'
    elif rml_rule['gatherAs'] == RDF_SEQ:
        collection = 'Seq'
    elif rml_rule['gatherAs'] == RDF_ALT:
        collection = 'Alt'
    
    #We define the value of the subject (at the start)
    subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[0, references[0]]))

    # This counter allow us to "name" the predicates for the container
    count=1

    # Loop through the dataframe containing the data
    for i in range(len(results_df[rml_rule['gather_reference']])):

        if i < len(results_df) - 1:
            #if the current value and the next value have the same key --> start or continuation of a container --> The next subject will be the same as the current subject (IRI)
            if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):

                df2 = df2._append({
                    'subject': f"<{subject_value}>",
                    'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                    'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                }, ignore_index=True)

                count+=1

            #if the current value and the next value do NOT have the same key --> end of container --> next subject will be a different IRI                
            elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: 

                #Empty container
                if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                    }, ignore_index=True)

                #Non-empty container
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

                #Definition of the new subject for the next triple  
                subject_value = rml_rule['object_map_value'].replace(f'{{{references[0]}}}', str(results_df.loc[i+1, references[0]]))

        #the current value is the last in the dataframe --> end of container            
        else:
            #Empty container
            if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):
                    df2 = df2._append({
                        'subject': f"<{subject_value}>",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                    }, ignore_index=True)
            #Non-empty container
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
    return df2


def fill_dataframe_as_unnamed_collection(results_df, rml_rule, df2, references):

    #Subject blank nodes incremented using a counter
    bnode_counter_subject=1
    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
    subject_value=bnode_subject

    #Blank nodes incremented using a counter
    bnode_counter=1

    # Loop through the dataframe containing the data 
    for i in range(len(results_df[rml_rule['gather_reference']])):
        
        if i < len(results_df) - 1:
            #if the current value and the next value have the same key in the json file --> start or continue a list --> The next subject will be a blank node
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

            #if the current value and the next value do NOT have the same key  --> end of list --> next subject will be a blank node
            elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: 
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

        #the current value is the last in the dataframe --> end of list or start of a new list of one element            
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
    return df2





def fill_dataframe_as_unnamed_container(results_df, rml_rule, df2, references):
    
    #We define the value of the subject (at the start)
    #Subject blank nodes incremented using a counter
    bnode_counter_subject=1
    bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
    subject_value=bnode_subject


    #Determine which container we have
    if rml_rule['gatherAs'] == RDF_BAG:
        collection = 'Bag'
    elif rml_rule['gatherAs'] == RDF_SEQ:
        collection = 'Seq'
    elif rml_rule['gatherAs'] == RDF_ALT:
        collection = 'Alt'

    # This counter allow us to "name" the predicates for the container
    count=1

    # Loop through the dataframe containing the data
    for i in range(len(results_df[rml_rule['gather_reference']])):
        
        if i < len(results_df) - 1:
            #if the current value and the next value have the same key --> start or continuation of a container --> The next subject will be the same as the current subject (Blank node)
            if (results_df.loc[i, references[0]])==(results_df.loc[i+1, references[0]]):

                df2 = df2._append({
                    'subject': f"{subject_value}",
                    'predicate': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_{count}>",
                    'object': f'"{results_df.loc[i, rml_rule['gather_reference']]}"'
                }, ignore_index=True)

                count+=1


            #if the current value and the next value do NOT have the same key --> end of container --> next subject will be a different blank node                
            elif results_df.loc[i, references[0]]!= results_df.loc[i+1, references[0]]: 
                
                
                #Empty container
                if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                    df2 = df2._append({
                        'subject': f"{subject_value}",
                        'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                    }, ignore_index=True)

                #Non-empty container
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

                #Definition of the new subject for the next triple  
                bnode_counter_subject +=1
                bnode_subject = f"_:bnode_subject{bnode_counter_subject}"
                subject_value=bnode_subject

        #the current value is the last in the dataframe --> end of container            
        else:
            #Empty container
            if pd.isna(results_df.loc[i, rml_rule['gather_reference']]):

                df2 = df2._append({
                    'subject': f"{subject_value}",
                    'predicate': "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    'object': f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#{collection}>"
                }, ignore_index=True)

            #Non-empty container
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
    return df2