# !/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import json
import linereader

TEST_ENTITIES_LIST = [
    {
        "id": "Q60",
        "type": "item",
        "ru_label": 'Нью Йорк',
        "en_label": 'New York',
        "ru_description": 'город Нью Йорк',
        "en_description": 'The New York city'
    },
    {
        "id": "Q61",
        "type": "item",
        "ru_label": '{}',
        "en_label": '{}',
        "ru_description": '{}',
        "en_description": '{}'
    },
    {
        "id": "Q62",
        "type": "item",
        "ru_label": '{}',
        "en_label": '{}',
        "ru_description": '{}',
        "en_description": '{}'
    }
]

TEST_ALIASES = [
    {
        "language": "en",
        "value": "NYC"
    },
    {
        "language": "en",
        "value": "New York"
    },
    {
        "language": "fr",
        "value": "New York City"
    },
    {
        "language": "fr",
        "value": "NYC"
    },
    {
        "language": "fr",
        "value": "The City"
    },
    {
        "language": "fr",
        "value": "City of New York"
    },
    {
        "language": "fr",
        "value": "La grosse pomme"
    }
]


def clear_query():
    """
    Clear DB
    :return:
    """
    return 'MATCH (n) DETACH DELETE n'


def stringify_dict(dict_):
    """

    :param dict_:
    :return:
    """
    string = '{'
    for key, val in dict_.items():
        string += str(key) + ":'" + str(val) + "',"
    return string[:-1] + '}'


def stringify_list(list_):
    """

    :param list_:
    :return:
    """
    string = '['
    for item in list_:
        string += str(item) + ','
    return string[:-1] + ']'


def items_query(entities_list):
    """

    :param entities_list:
    :return:
    """
    template = "UNWIND %s AS entity MERGE (e:Item { id: entity.id,  ru_label: entity.ru_label, en_label: entity.en_label, ru_description: entity.ru_description, en_description: entity.en_description})"
    entities_list = [stringify_dict(x) for x in entities_list]
    return template % stringify_list(entities_list)


def properties_query(properties_list):
    """

    :param properties_list:
    :return:
    """
    template = "UNWIND %s AS property MERGE (e:Property { id: property.id,  ru_label: property.ru_label, en_label: property.en_label, ru_description: property.ru_description, en_description: property.en_description})"
    properties_list = [stringify_dict(x) for x in properties_list]
    return template % stringify_list(properties_list)


def aliases_query(entity_id, aliases_list):
    """

    :param entity_id:
    :param aliases_list:
    :return:
    """
    template = "UNWIND %s AS alias MERGE (a:Alias {id: alias.id, language: alias.language, value:alias.value})"
    n = 0
    aliases_list_ = []
    for alias in aliases_list:
        alias['id'] = 'A' + entity_id + '-' + str(n)
        aliases_list_.append(alias)
        n += 1
    aliases_list = aliases_list_
    aliases_list = [stringify_dict(x) for x in aliases_list]
    return [alias.get('id') for alias in aliases_list_], template % stringify_list(aliases_list)


def aliases_relations_query(entity_id, aliases_ids):
    """
    :param entity_id:
    :param aliases_ids:
    :return:
    """
    template = "UNWIND %s AS relation MATCH (e:Entity {id: relation.Entity}) MATCH (a:Alias {id: relation.Alias}) CREATE (e) <-[:ALIAS_OF]-(a)"
    relations_list = list()
    for alias_id in aliases_ids:
        relations_list.append(stringify_dict(dict_={
            "Entity": entity_id,
            "Alias": alias_id
        }))
    relations = stringify_list(relations_list)
    return template % relations


def flush(query, driver):
    """

    :param query:
    :param driver:
    :return:
    """
    with driver.session() as session:
        try:
            session.run(query)
        except Exception as e:
            logging.error(e)


def process_line(line):
    """

    :param line:
    :return:
    """
    entity = json.loads(line)
    type = entity.get('type')
    id = entity.get('id')
    if type is 'item':
        ru_label = entity.get('label').get('ru')
        en_label = entity.get('label').get('en')
    else:
        ru_label = entity.get('label').get('ru').replace(' ', '_')
        en_label = entity.get('label').get('en').replace(' ', '_')
    ru_description = entity.get('description').get('ru')
    en_description = entity.get('description').get('en')
    aliases_list = list()
    ru_aliases = entity.get('aliases').get('ru')
    en_aliases = entity.get('aliases').get('en')
    if ru_aliases:
        aliases_list.extend(ru_aliases)
    if en_aliases:
        aliases_list.extend(en_aliases)
    if not (ru_label or en_label):
        return None
    return {
        'type': type,
        'id': id,
        'ru_label': ru_label,
        'en_label': en_label,
        'ru_description': ru_description,
        'en_description': en_description,
        'aliases_list': aliases_list
    }
