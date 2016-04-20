import sys
import csv
import json
import uuid
import argparse
import contextlib

id_prop_name = '_id'


@contextlib.contextmanager
def smart_open(f, mode='r'):
    if isinstance(f, str):
        f = open(f, mode)
    try:
        yield f
    finally:
        if f not in (sys.stdin, sys.stdout):
            f.close()

def iterables_to_csv(iterables, filename=sys.stdout):
    with smart_open(filename, 'w') as f:
        writer = csv.writer(f)
        for iterable in iterables:
            writer.writerow(iterable)

def entities_from_triples(triples):
    keyed_entities = {}
    for triple in triples:
        entity = keyed_entities.setdefault(triple[0],
                                           {id_prop_name: triple[0]})
        entity[triple[1]] = triple[2]
    for entity_id, entity in keyed_entities.items():
        yield entity

def entities_to_ndjson(entities, filename=sys.stdout):
    with smart_open(filename, 'w') as f:
        for entity in entities:
            f.write(json.dumps(entity) + '\n')

def entities_to_csv(entities, filename=sys.stdout):
    with smart_open(filename, 'w') as f:
        entity = next(entities)
        fields = list(entity.keys())
        if id_prop_name in fields:
            current = fields.index(id_prop_name)
            fields[0], fields[current] = fields[current], fields[0]
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        writer.writerow(entity)
        for entity in entities:
            writer.writerow(entity)

def bool_from_str(string):
    if string == 'True':
        return True
    if string == 'False':
        return False
    raise ValueError('must be True or False')

def booled_tuples_from_csv(filename=sys.stdin):
    with smart_open(filename) as f:
        for row in csv.reader(f):
            row[-1] = bool_from_str(row[-1])
            yield tuple(row)

def entities_from_csv(filename):
    with open(filename) as f:
        for entity in csv.DictReader(f):
            yield entity

def entities_from_ndjson(filename):
    with open(filename) as f:
        for line in f:
            entity = json.loads(line)
            yield entity

def triples_from_entities(entities):
    for entity in entities:
        entity_id = entity[id_prop_name]  # Global!
        for prop, val in entity.items():
            if prop == id_prop_name:
                continue
            yield entity_id, prop, val

def triples_from_csv(filename):
    entities = entities_from_csv(filename)
    triples = triples_from_entities(entities)
    for triple in triples:
        yield triple

def triples_from_ndjson(filename):
    entities = entities_from_ndjson(filename)
    triples = triples_from_entities(entities)
    for triple in triples:
        yield triple

def triples_from_dor(filename, commit_id=None):
    quints = booled_tuples_from_csv(filename)
    quads = quads_through_commit(quints, commit_id)
    triples = triples_from_quads(quads)
    return triples

def triples_from_filename(filename):
    ndjson = filename.endswith('.ndjson')
    jsonl = filename.endswith('.jsonl')
    if filename.endswith('.dor'):
        triples = triples_from_dor(filename)
    elif ndjson or jsonl:
        triples = triples_from_ndjson(filename)
    else:
        triples = triples_from_csv(filename)
    for triple in triples:
        yield triple

def quad_diff_triples(start, finish):
    # This assumes the triples are _all_ the triples for each side.
    start, finish = set(start), set(finish)
    retractions = start - finish
    for retraction in retractions:
        yield retraction + (False,)
    additions = finish - start
    for addition in additions:
        yield addition + (True,)

def triples_from_quads(quads):
    triples = set()
    for quad in quads:
        if quad[3]:  # addition
            triples.add(quad[:3])
        else:
            triples.remove(quad[:3])
    return triples

def make_commit_id():
    return str(uuid.uuid4())

def quints_from_quads(quads):
    commit_id = make_commit_id()
    for quad in quads:
        yield (commit_id,) + quad

def quads_through_commit(quints, commit_id=None):
    if commit_id is not None:
        saw_last = False
    for quint in quints:
        if commit_id is not None:
            if quint[0].startswith(commit_id):
                saw_last = True
            elif saw_last:
                break
        yield quint[1:]

def diff(start_filename, finish_filename):
    if finish_filename is None:
        start_triples = []
        finish_triples = list(triples_from_filename(start_filename))
    else:
        start_triples = triples_from_filename(start_filename)
        finish_triples = triples_from_filename(finish_filename)
    quads = quad_diff_triples(start_triples, finish_triples)
    iterables_to_csv(quads)

def commit(filename):
    quads = booled_tuples_from_csv(filename)
    quints = quints_from_quads(quads)
    iterables_to_csv(quints)

def log(filename):
    quints = booled_tuples_from_csv(filename)
    current_commit = None
    for quint in quints:
        if quint[0] != current_commit:
            current_commit = quint[0]
            print(current_commit)

def checkout(filename, commit_id=None, form='csv'):
    triples = triples_from_dor(filename, commit_id=commit_id)
    entities = entities_from_triples(triples)
    if 'json' in form:
        entities_to_ndjson(entities)
    else:
        entities_to_csv(entities)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command')

p_diff = subparsers.add_parser('diff', help='diff one file to next')
p_diff.add_argument('start', help='filename of base dataset')
p_diff.add_argument('finish', help='filename of updated dataset', nargs='?')

p_commit = subparsers.add_parser('commit', help='make a diff into a commit')
p_commit.add_argument('filename', help='filename of a diff to commit',
                      nargs='?', default=sys.stdin)

p_log = subparsers.add_parser('log', help='show commits available')
p_log.add_argument('filename', help='filename of dor repository')

p_co = subparsers.add_parser('checkout', help='get a version of data')
p_co.add_argument('filename', help='filename of dor repository')
p_co.add_argument('commit', help='identifier for a version', nargs='?')
p_co.add_argument('--form', help='default csv or [nd]json[l]', default='csv')

def main():
    args = parser.parse_args()
    if args.command == 'diff':
        diff(args.start, args.finish)
    if args.command == 'commit':
        commit(args.filename)
    if args.command == 'log':
        log(args.filename)
    if args.command == 'checkout':
        checkout(args.filename, args.commit, args.form)

if __name__ == '__main__':
    main()
