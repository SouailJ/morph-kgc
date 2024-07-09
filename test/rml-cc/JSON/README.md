# RML-CC test cases

## RMLTC-CC-0001
- RMLTC-CC-0001-Alt: unnamed alternative
- RMLTC-CC-0001-Bag: unnamed bag
- RMLTC-CC-0001-List: unnamed list
- RMLTC-CC-0001-Seq: unnamed sequence

## RMLTC-CC-0002
- RMLTC-CC-0002-Alt: named alternative
- RMLTC-CC-0002-Bag: named bag
- RMLTC-CC-0002-List: named list
- RMLTC-CC-0002-Seq: named sequence

## RMLTC-CC-0003
- RMLTC-CC-0003-EA: empty alternative (rml:allowEmptyListAndContainer = true)
- RMLTC-CC-0003-EB: empty bag (rml:allowEmptyListAndContainer = true)
- RMLTC-CC-0003-EL: empty list (rml:allowEmptyListAndContainer = true)
- RMLTC-CC-0003-ES: empty sequence (rml:allowEmptyListAndContainer = true)

- RMLTC-CC-0003-NEA: do not generate empty alternative (default)
- RMLTC-CC-0003-NEAb: do not generate empty alternative (rml:allowEmptyListAndContainer = false)

- RMLTC-CC-0003-NEB: do not generate empty bag (default)
- RMLTC-CC-0003-NEBb: do not generate empty bag (rml:allowEmptyListAndContainer = false)

- RMLTC-CC-0003-NEL: do not generate empty list (default)
- RMLTC-CC-0003-NELb: do not generate empty list (rml:allowEmptyListAndContainer = false)

- RMLTC-CC-0003-NES: do not generate empty sequence (default)
- RMLTC-CC-0003-NESb: do not generate empty sequence (rml:allowEmptyListAndContainer = false)

## RMLTC-CC-0004
- RMLTC-CC-0004-SMA1: unnamed alternative as a subject, no predicate-object map
- RMLTC-CC-0004-SMA2: unnamed alternative as a subject, no predicate-object map, do not generate empty alternative
- RMLTC-CC-0004-SMA2E: unnamed alternative as a subject, no predicate-object map, generate empty alternative

- RMLTC-CC-0004-SMA3: unnamed alternative as a subject, with a predicate-object map
- RMLTC-CC-0004-SMA4: unnamed alternative as a subject, with a predicate-object map, do not generate empty alternative
- RMLTC-CC-0004-SMA4E: unnamed alternative as a subject, with a predicate-object map, generate empty alternative

- RMLTC-CC-0004-SMB1: unnamed bag as a subject, no predicate-object map
- RMLTC-CC-0004-SMB2: unnamed bag as a subject, no predicate-object map, do not generate empty bag
- RMLTC-CC-0004-SMB2E: unnamed bag as a subject, no predicate-object map, generate empty bag

- RMLTC-CC-0004-SMB3: unnamed bag as a subject, with a predicate-object map
- RMLTC-CC-0004-SMB4: unnamed bag as a subject, with a predicate-object map, do not generate empty bag
- RMLTC-CC-0004-SMB4E: unnamed bag as a subject, with a predicate-object map, generate empty bag

- RMLTC-CC-0004-SML1: unnamed list as a subject, no predicate-object map
- RMLTC-CC-0004-SML2: unnamed list as a subject, no predicate-object map, do not generate empty list
- RMLTC-CC-0004-SML2E: unnamed list as a subject, no predicate-object map, generate empty list
  
- RMLTC-CC-0004-SML3: unnamed list as a subject, with a predicate-object map
- RMLTC-CC-0004-SML4: unnamed list as a subject, with a predicate-object map, do not generate empty list
- RMLTC-CC-0004-SML4E: unnamed list as a subject, with a predicate-object map, generate empty list

- RMLTC-CC-0004-SMS1: unnamed sequence as a subject, no predicate-object map
- RMLTC-CC-0004-SMS2: unnamed sequence as a subject, no predicate-object map, do not generate empty sequence
- RMLTC-CC-0004-SMS2E: unnamed sequence as a subject, no predicate-object map, generate empty sequence

- RMLTC-CC-0004-SMS3: unnamed sequence as a subject, with a predicate-object map
- RMLTC-CC-0004-SMS4: unnamed sequence as a subject, with a predicate-object map, do not generate empty sequence
- RMLTC-CC-0004-SMS4E: unnamed sequence as a subject, with a predicate-object map, generate empty sequence

## RMLTC-CC-0005
- RMLTC-CC-0005-AApp1: multi-valued gather map with append strategy for an alternative
- RMLTC-CC-0005-AApp2: multi-valued gather map with append strategy for an alternative, do not generate empty alternative
- RMLTC-CC-0005-ACar1: multi-valued gather map with cartesian product strategy for an alternative
- RMLTC-CC-0005-ACar2: multi-valued gather map with cartesian product strategy for an alternative, do not generate empty alternative

- RMLTC-CC-0005-BApp1: multi-valued gather map with append strategy for a bag
- RMLTC-CC-0005-BApp2: multi-valued gather map with append strategy for a bag, do not generate empty bag
- RMLTC-CC-0005-BCar1: multi-valued gather map with cartesian product strategy for a bag
- RMLTC-CC-0005-BCar2: multi-valued gather map with cartesian product strategy for a bag, do not generate empty bag

- RMLTC-CC-0005-LApp1: multi-valued gather map with append strategy for a list
- RMLTC-CC-0005-LApp2: multi-valued gather map with append strategy for a list, do not generate empty list
- RMLTC-CC-0005-LCar1: multi-valued gather map with cartesian product strategy for a list
- RMLTC-CC-0005-LCar2: multi-valued gather map with cartesian product strategy for a list, do not generate empty list

- RMLTC-CC-0005-SApp1: multi-valued gather map with append strategy for a sequence
- RMLTC-CC-0005-SApp2: multi-valued gather map with append strategy for a sequence, do not generate empty sequence
- RMLTC-CC-0005-SCar1: multi-valued gather map with cartesian product strategy for a sequence
- RMLTC-CC-0005-SCar2: multi-valued gather map with cartesian product strategy for a sequence, do not generate empty sequence

## RMLTC-CC-0006
- RMLTC-CC-0006-ITA0: unnamed alternatives generated across multiple iterations
- RMLTC-CC-0006-ITA1: named alternatives generated across multiple iterations
- RMLTC-CC-0006-ITA2: unnamed alternatives generated across multiple iterations with multi-valued gather map
- RMLTC-CC-0006-ITA3: named alternatives generated across multiple iterations with multi-valued gather map

- RMLTC-CC-0006-ITB0: unnamed bags generated across multiple iterations
- RMLTC-CC-0006-ITB1: named bags generated across multiple iterations
- RMLTC-CC-0006-ITB2: unnamed bags generated across multiple iterations with multi-valued gather map
- RMLTC-CC-0006-ITB3: named bags generated across multiple iterations with multi-valued gather map

- RMLTC-CC-0006-ITL0: unnamed lists generated across multiple iterations
- RMLTC-CC-0006-ITL1: named lists generated across multiple iterations
- RMLTC-CC-0006-ITL2: unnamed lists generated across multiple iterations with multi-valued gather map
- RMLTC-CC-0006-ITL3: named lists generated across multiple iterations with multi-valued gather map

- RMLTC-CC-0006-ITS0: unnamed sequences generated across multiple iterations
- RMLTC-CC-0006-ITS1: named sequences generated across multiple iterations
- RMLTC-CC-0006-ITS2: unnamed sequences generated across multiple iterations with multi-valued gather map
- RMLTC-CC-0006-ITS3: named sequences generated across multiple iterations with multi-valued gather map

## RMLTC-CC-0007
- RMLTC-CC-0007-AA: alternative of alternatives (nested containers and collections)
- RMLTC-CC-0007-AB: alternative of bags (nested containers and collections)
- RMLTC-CC-0007-AL: alternative of lists (nested containers and collections)
- RMLTC-CC-0007-AS: alternative of sequences (nested containers and collections)

- RMLTC-CC-0007-BA: bag of alternatives (nested containers and collections)
- RMLTC-CC-0007-BB: bag of bags (nested containers and collections)
- RMLTC-CC-0007-BL: bag of lists (nested containers and collections)
- RMLTC-CC-0007-BS: bag of sequences (nested containers and collections)

- RMLTC-CC-0007-LA: list of alternatives (nested containers and collections)
- RMLTC-CC-0007-LB: list of bags (nested containers and collections)
- RMLTC-CC-0007-LL: list of lists (nested containers and collections)
- RMLTC-CC-0007-LS: list of sequences (nested containers and collections)

- RMLTC-CC-0007-SA: sequence of alternatives (nested containers and collections)
- RMLTC-CC-0007-SB: sequence of bags (nested containers and collections)
- RMLTC-CC-0007-SL: sequence of lists (nested containers and collections)
- RMLTC-CC-0007-SS: sequence of sequences (nested containers and collections)

## RMLTC-CC-0008
- RMLTC-CC-0008-AJoin: RDF alternative whose members are generated by a referencing object-map with join condition
- RMLTC-CC-0008-ANoJoin: RDF alternative whose members are generated by a referencing object-map with default join condition

- RMLTC-CC-0008-BJoin: RDF bag whose members are generated by a referencing object-map with join condition
- RMLTC-CC-0008-BNoJoin: RDF bag whose members are generated by a referencing object-map with default join condition

- RMLTC-CC-0008-LJoin: RDF list whose members are generated by a referencing object-map with join condition
- RMLTC-CC-0008-LNoJoin: RDF list whose members are generated by a referencing object-map with default join condition

- RMLTC-CC-0008-SJoin: RDF sequence whose members are generated by a referencing object-map with join condition
- RMLTC-CC-0008-SNoJoin: RDF sequence whose members are generated by a referencing object-map with default join condition

## RMLTC-CC-0009
- RMLTC-CC-0009-DUP-Alt: RDF alternative that contains duplicates (should test correct behavior of multi-valued expression maps)
- RMLTC-CC-0009-DUP-Bag: RDF bag that contains duplicates (should test correct behavior of multi-valued expression maps)
- RMLTC-CC-0009-DUP-List: RDF list that contains duplicates (should test correct behavior of multi-valued expression maps)
- RMLTC-CC-0009-DUP-Seq: RDF sequence that contains duplicates (should test correct behavior of multi-valued expression maps)

## RMLTC-CC-0010
- RMLTC-CC-0010-ListGMa: Combining graph maps and gather maps (a blank node for each list)
- RMLTC-CC-0010-ListGMb: Combining graph maps and gather maps (lists are merged via blank node identifiers)

- RMLTC-CC-0010-AltGMa: Combining graph maps and gather maps (a blank node for each alternative)
- RMLTC-CC-0010-AltGMb: Combining graph maps and gather maps (alternatives are merged via blank node identifiers)

- RMLTC-CC-0010-BagGMa: Combining graph maps and gather maps (a blank node for each bag)
- RMLTC-CC-0010-BagGMb: Combining graph maps and gather maps (bags are merged via blank node identifiers)

- RMLTC-CC-0010-SeqGMa: Combining graph maps and gather maps (a blank node for each sequence)
- RMLTC-CC-0010-SeqGMb: Combining graph maps and gather maps (sequences are merged via blank node identifiers)
