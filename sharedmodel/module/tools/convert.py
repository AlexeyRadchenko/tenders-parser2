
def list_to_dict(entities):
    result = dict()

    if not entities:
        return result

    for i, entity in enumerate(entities):
        entity_json = entity.to_dict()
        if not entity_json:
            continue
        result[str(i)] = entity_json

    return result


def list_to_array(entities):
    result = []

    if not entities:
        return result

    for entity in entities:
        entity_json = entity.to_dict()
        if not entity_json:
            continue
        result.append(entity_json)

    return result
