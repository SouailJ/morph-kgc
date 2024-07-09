__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


##############################################################################
#######################   RML DATAFRAME COLUMNS   ############################
##############################################################################

RML_DATAFRAME_COLUMNS = [
    'source_name', 'triples_map_id', 'triples_map_type', 'logical_source_type', 'logical_source_value', 'iterator',
    'reference_formulation',
    'subject_map_type', 'subject_map_value', 'subject_termtype',
    'predicate_map_type', 'predicate_map_value',
    'object_map_type', 'object_map_value', 'object_termtype',
    'lang_datatype', 'lang_datatype_map_type', 'lang_datatype_map_value',
    'graph_map_type', 'graph_map_value',
    'subject_join_conditions', 'object_join_conditions',
    'gather', 'gather_subject', 'gather_reference', 'gather_references', 'gather_node', 'gatherAs', 'gatherAs_subject', 'strategy', 'allowEmptyListAndContainer'
]

##############################################################################
#######################   FNML DATAFRAME COLUMNS   ############################
##############################################################################

FNML_DATAFRAME_COLUMNS = [
    'function_execution', 'function_map_value', 'parameter_map_value', 'value_map_type', 'value_map_value'
]


##############################################################################
########################   RML PARSING QUERIES   #############################
##############################################################################

RML_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>
    prefix sd: <https://w3id.org/okn/o/sd#>

    SELECT DISTINCT 
        ?triples_map_id ?triples_map_type ?logical_source_type ?logical_source_value ?iterator ?reference_formulation
        ?subject_map_type ?subject_map_value ?subject_map ?subject_termtype
        ?predicate_map_type ?predicate_map_value
        ?object_map_type ?object_map_value ?object_map ?object_termtype
        ?lang_datatype ?lang_datatype_map_type ?lang_datatype_map_value
        ?graph_map_type ?graph_map_value
        ?gather ?gather_subject ?gather_reference ?gather_node ?gatherAs ?gatherAs_subject ?strategy ?allowEmptyListAndContainer
        (GROUP_CONCAT(?gather_reference; separator=", ") AS ?gather_references)

    WHERE {
        ?triples_map_id rml:logicalSource ?_source ;
                        a ?triples_map_type .
        OPTIONAL {
            # logical_source is optional because it can be specified with file_path in config (see #119)
            ?_source ?logical_source_type ?logical_source_value .
            OPTIONAL {
                ?logical_source_value sd:name ?logical_source_in_memory_value.
                BIND(CONCAT("{",?logical_source_in_memory_value,"}") AS ?logical_source_value)
            }
            FILTER ( ?logical_source_type IN ( rml:source, rml:tableName, rml:query ) ) .
        }
        OPTIONAL { ?_source rml:iterator ?iterator . }
        OPTIONAL { ?_source rml:referenceFormulation ?reference_formulation . }

    # Subject -------------------------------------------------------------------------
        ?triples_map_id rml:subjectMap ?subject_map .
        ?subject_map ?subject_map_type ?subject_map_value .
        FILTER ( ?subject_map_type IN (
                            rml:constant, rml:template, rml:reference, rml:quotedTriplesMap, rml:functionExecution, rml:gather, rml:gatherAs, rml:strategy, rml:allowEmptyListAndContainer ) ) .
        OPTIONAL { ?subject_map rml:termType ?subject_termtype . }
        
        # Added for RML-CC
        OPTIONAL { 
            ?subject_map rml:gather ?gather_subject .
            ?subject_map rml:gatherAs ?gatherAs_subject .
        } 

        OPTIONAL {
            ?gather_subject rdf:rest*/rdf:first ?gather_node .
            ?gather_node rml:reference ?gather_reference .
        }

    # Predicate -----------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rml:predicateObjectMap ?_predicate_object_map .
            ?_predicate_object_map rml:predicateMap ?_predicate_map .
            ?_predicate_map ?predicate_map_type ?predicate_map_value .
            FILTER ( ?predicate_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .

    # Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                
                OPTIONAL {
                    ?object_map ?object_map_type ?object_map_value .

                    #Added: allows objectMap to have no type (so that rml:gather is not null)
                    FILTER (!BOUND(?object_map_type) || ?object_map_type IN (
                                rml:constant, rml:template, rml:reference, rml:quotedTriplesMap, rml:functionExecution ) ) .
                }

                OPTIONAL { ?object_map rml:termType ?object_termtype . }
                OPTIONAL {
                    ?object_map ?lang_datatype ?lang_datatype_map .
                    ?lang_datatype_map ?lang_datatype_map_type ?lang_datatype_map_value .
                    # remove xsd:string data types as it is equivalent to not specifying any data type
                    FILTER ( ?lang_datatype_map_value != <http://www.w3.org/2001/XMLSchema#string> ) .
                    FILTER ( ?lang_datatype_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .
                }
            
            # Added for RML-CC
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map rml:gather ?gather .
                OPTIONAL {
                    ?gather rdf:rest*/rdf:first ?gather_node .
                    ?gather_node rml:reference ?gather_reference .
                }
                ?object_map rml:gatherAs ?gatherAs .
                OPTIONAL {
                    ?object_map rml:strategy ?strategy .
                }
                BIND(COALESCE(?strategy, rml:append) AS ?strategy)
                FILTER ( ?strategy IN (
                            rml:append, rml:cartesianProduct ) ) .

                OPTIONAL {
                    ?object_map rml:allowEmptyListAndContainer ?allowEmptyListAndContainer .
                }
                BIND(IF(BOUND(?allowEmptyListAndContainer), xsd:boolean(?allowEmptyListAndContainer), false) AS ?allowEmptyListAndContainer)
                FILTER ( ?allowEmptyListAndContainer IN (false, true) ) .

            }
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map rml:parentTriplesMap ?object_map_value .
                OPTIONAL { ?object_map rml:termType ?object_termtype . }
                BIND ( rml:parentTriplesMap AS ?object_map_type ) .
            }
            OPTIONAL {
                ?_predicate_object_map rml:graphMap ?graph_map .
                ?graph_map ?graph_map_type ?graph_map_value .
                FILTER ( ?graph_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .
            }
        }
    }
    GROUP BY ?gather 
    
"""

RML_JOIN_CONDITION_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>

    SELECT DISTINCT ?term_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?term_map rml:joinCondition ?join_condition .
        ?join_condition rml:child ?child_value; rml:parent ?parent_value .
    }
"""


##############################################################################
########################   FNML PARSING QUERY   ###############################
##############################################################################

FNML_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>

    SELECT DISTINCT
        ?function_execution ?function_map_value ?parameter_map_value ?value_map_type ?value_map_value

    WHERE {

    # FuntionMap ----------------------------------------------------------------------

        ?function_execution rml:functionMap ?function_map .        
        ?function_map rml:constant ?function_map_value .

        # return maps are not used in the current implementation, default is first return value

    # Input ---------------------------------------------------------------------------

        OPTIONAL {
            # OPTIONAL because a function can have 0 arguments (e.g., uuid())
            ?function_execution rml:input ?input .

            ?input rml:parameterMap ?parameter_map .
            ?parameter_map rml:constant ?parameter_map_value .

            ?input rml:inputValueMap ?value_map .
            ?value_map ?value_map_type ?value_map_value .
            FILTER ( ?value_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .
        }
    }
"""
