from collections.abc import MutableMapping

def batch_iterator(items, batch_size):
  for idx in range(0, len(items), batch_size):
    yield items[idx:idx + batch_size]

def flatten(dict_object, parent_key = '', seperator = '_'):
  items = []
  for key, value in dict_object.items():
    new_key = parent_key + seperator + key if parent_key else key

    if isinstance(value, MutableMapping):
      items.extend(
        flatten(value, parent_key = new_key, seperator = seperator).items()
      )
    else:
      items.append((new_key, value))

  return dict(items)
