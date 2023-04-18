# from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from dynamodb_json import json_util

ERC1167_BYTECODE_WHITELIST = {
    "0x363d3d373d3d3d363d73e38f942db7a1b4213d6213f70c499b59287b01f15af43d82803e903d91602b57fd5bf3":"Foundation",
}

ERC1967_PROXY_WHITELIST = {
    "0xe4e4003afe3765aca8149a82fc064c0b125b9e5a":"manifold.xyz ERC721 Creator Implementation",
    "0x142fd5b9d67721efda3a5e2e9be47a96c9b724a4":"manifold.xyz ERC1155 Creator Implementation",
}

# def python_obj_to_dynamo_obj(python_obj: dict) -> dict:
#     serializer = TypeSerializer()
#     return {
#         k: serializer.serialize(v)
#         for k, v in python_obj.items()
#     }

# def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
#     deserializer = TypeDeserializer()
#     return {
#         k: deserializer.deserialize(v) 
#         for k, v in dynamo_obj.items()
#     }  

# For detailed/quick metadata
# --------------------------------
def read_fixed_json(file_name:str) -> dict:
    """Fix and read a json file

    :param file_name: _description_
    :type file_name: str
    :return: _description_
    :rtype: dict
    """
    # read data by line
    with open(file_name, 'r') as f:
        data = f.read()
    # add square bracket at the beginning and end of the file
    data = '[' + data + ']'
    # add comma at the end of each line except the last line
    data = data.replace('}\n', '},\n')
    data = data.replace('},\n]', '}\n]')
    # with open(file_name, 'w') as f:
    #     f.write(data)
    return json_util.loads(data)

def read_dynamo_json(file_name: str, mode='quick') -> list:
    """read dynamo json file (quick/detailed metadata)

    :param file_name: _description_
    :type file_name: str
    :return: _description_
    :rtype: list
    """
    data = read_fixed_json(file_name)
    item_key = 'Item'
    if mode == 'quick':
        return [d[item_key] for d in data]
    # detailed metadata
    return [process_collection_metadata(d[item_key]) for d in data]
    

# For detailed metadata
# --------------------------------
def get_value(json_data, nested_keys):
    try: value = json_data[nested_keys[0]]
    except: return None
    if len(nested_keys) <= 1 or value is None:
        return value
    return get_value(value, nested_keys[1:])

def get_best(json_data, keys_list):
    return get_value(json_data, next(filter(lambda v: get_value(json_data, v) is not None, keys_list), None))

def process_collection_metadata(data: dict):
    """Generate quick metadata

    :param data: _description_
    :type data: dict
    :return: _description_
    :rtype: _type_
    """
    quick_meta = {}
    # data = dynamo_obj_to_python_obj(data)
    
    quick_meta['pk'] = data['pk']
    
    quick_meta['contract_address'] = data['pk'][-42:]

    # contract
    quick_meta['contract_schema'] = get_best(data, [["override", "contract_schema"],
                                                    ["moralis_metadata", "contract_type"],
                                                    ["opensea_asset", "schema_name"],
                                                   ])

    quick_meta['contract_ticker_symbol'] = get_best(data, [["override", "contract_ticker_symbol"],
                                                           ["moralis_metadata", "symbol"],
                                                           ["opensea_asset", "symbol"],
                                                          ])

    # display
    quick_meta['display_name'] = get_best(data, [["override", "display_name"],
                                                 ["opensea_asset", "collection", "name"],
                                                 ["opensea_asset", "name"],
                                                 ["moralis_metadata", "name"],
                                                ])

    quick_meta['description'] = get_best(data, [["override", "description"],
                                                ["opensea_asset", "description"],
                                                ["opensea_collection", "description"],
                                               ])

    quick_meta['banner_image_url'] = get_best(data, [["override", "banner_image_url"],
                                                     ["opensea_asset", "collection", "banner_image_url"],
                                                     ["opensea_collection", "banner_image_url"],
                                                    ])

    quick_meta['collection_image_url'] = get_best(data, [["override", "collection_image_url"],
                                                         ["opensea_asset", "collection", "image_url"],
                                                         ["opensea_collection", "image_url"],
                                                        ])

    # external
    quick_meta['external'] = {}

    quick_meta['external']['discord'] = get_best(data, [["override", "external", "discord"],
                                                        ["opensea_asset", "collection", "discord_url"],
                                                        ["opensea_collection", "discord_url"],
                                                       ])

    quick_meta['external']['instagram'] = get_best(data, [["override", "external", "instagram"],
                                                          ["opensea_asset", "collection", "instagram_username"],
                                                          ["opensea_collection", "instagram_username"],
                                                         ])

    quick_meta['external']['telegram'] = get_best(data, [["override", "external", "telegram"],
                                                         ["opensea_asset", "collection", "telegram_url"],
                                                         ["opensea_collection", "telegram_url"],
                                                        ])

    quick_meta['external']['twitter'] = get_best(data, [["override", "external", "twitter"],
                                                        ["opensea_asset", "collection", "twitter_username"],
                                                        ["opensea_collection", "twitter_username"],
                                                       ])

    quick_meta['external']['os_slug'] = get_best(data, [["override", "external", "os_slug"],
                                                        ["opensea_asset", "collection", "slug"],
                                                        ["opensea_collection", "slug"],
                                                       ])

    quick_meta['external']['urls'] = (get_value(data, ["override", "external", "urls"])
                                      or [x for x in set([
                                          get_best(data, [["opensea_asset", "collection", "external_url"],
                                                        ["opensea_collection", "external_url"],
                                                       ]),
                                          get_best(data, [["opensea_asset", "collection", "wiki_url"],
                                                        ["opensea_collection", "wiki_url"],
                                                       ]),
                                          ]) if x is not None
                                         ])

    # flags
    quick_meta['flags'] = {}

    quick_meta['flags']['os_hidden'] = get_best(data, [["override", "external", "os_hidden"],
                                                       ["opensea_asset", "collection", "hidden"],
                                                       ["opensea_collection", "hidden"],
                                                      ]) or False

    quick_meta['flags']['os_is_nsfw'] = get_best(data, [["override", "external", "os_is_nsfw"],
                                                        ["opensea_asset", "collection", "is_nsfw"],
                                                        ["opensea_collection", "is_nsfw"],
                                                       ]) or False

    quick_meta['flags']['os_safelist_state'] = get_best(data, [["override", "external", "os_safelist_state"],
                                                               ["opensea_asset", "collection", "safelist_request_status"],
                                                               ["opensea_collection", "safelist_request_status"],
                                                              ]) or "not_requested"

    # if 0 functions in ABI then it's not verified on etherscan
    quick_meta['flags']['code_verified'] = ((get_value(data, ["override", "external", "code_verified"]))
                                            or ((get_value(data, ["etherscan_abi", "num_functions"]) or 0) > 0))

    # does this contract pass the functions threshold? if num_function is less than 10, it VERY likely that
    # it cannot satisfy ERC-721 / 1155 standard properly
    quick_meta['flags']['functions_threshold'] = ((get_value(data, ["override", "external", "functions_threshold"]))
                                            or ((get_value(data, ["etherscan_abi", "num_functions"]) or 0) >= 10))

    # if more than 25 set approval for all for this contract (a lot of scam contracts don't have this method)
    quick_meta['flags']['approval_threshold'] = ((get_value(data, ["override", "external", "approval_threshold"]))
                                            or ((get_value(data, ["moralis_contract_approval", "total"]) or 0) > 25))

    # bytecode size can be a taletail sign since scammers optimize for this now and deploy cheap contracts
    quick_meta['flags']['bytecode_threshold'] = ((get_value(data, ["override", "external", "bytecode_threshold"]))
                                            or ((get_value(data, ["byte_code_size"]) or 0) > 1000))

    # if bytecode size is 0 that means contract no longer exists
    quick_meta['flags']['did_self_destruct'] = (get_value(data, ["byte_code_size"]) or 0) == 2
    
    # This will be set to True later if it's whitelisted
    quick_meta['flags']['is_whitelisted_proxy'] = False
    

    # stats
    quick_meta['display_stats'] = {}

    quick_meta['display_stats']['floor_price'] = get_best(data, [["override", "display_stats", "floor_price"],
                                                                 ["opensea_collection", "stats", "floor_price"],
                                                                ])

    quick_meta['display_stats']['supply'] = get_best(data, [["override", "display_stats", "supply"],
                                                            ["opensea_collection", "stats", "total_supply"],
                                                           ])

    quick_meta['display_stats']['holders'] = get_best(data, [["override", "display_stats", "holders"],
                                                             ["opensea_collection", "stats", "num_owners"],
                                                            ])

    quick_meta['display_stats']['one_day_volume'] = get_best(data, [["override", "display_stats", "one_day_volume"],
                                                                    ["opensea_collection", "stats", "one_day_volume"],
                                                                   ])

    quick_meta['display_stats']['seven_day_volume'] = get_best(data, [["override", "display_stats", "seven_day_volume"],
                                                                      ["opensea_collection", "stats", "seven_day_volume"],
                                                                     ])

    quick_meta['display_stats']['thirty_day_volume'] = get_best(data, [["override", "display_stats", "thirty_day_volume"],
                                                                       ["opensea_collection", "stats", "thirty_day_volume"],
                                                                      ])

    quick_meta['display_stats']['total_volume'] = get_best(data, [["override", "display_stats", "total_volume"],
                                                                  ["opensea_collection", "stats", "total_volume"],
                                                                 ])

    # display state
    quick_meta['is_nsfw'] = get_best(data, [["override", "is_nsfw"],
                                            ["opensea_asset", "collection", "is_nsfw"],
                                            ["opensea_collection", "is_nsfw"],
                                           ]) or False

    # Default fallback states
    quick_meta['display_state'] = "caution"
    quick_meta['display_state_reason'] = "Fallback state"
    
    if get_value(data, ["override", "display_state"]):
        quick_meta['display_state'] = get_value(data, ["override", "display_state"])
        quick_meta['display_state_reason'] = get_value(data, ["override", "display_state_reason"])
    elif quick_meta['flags']['did_self_destruct']:
        quick_meta['display_state'] = "deleted"
        quick_meta['display_state_reason'] = "The contract self-destructed"
    elif quick_meta['flags']['os_hidden']:
        quick_meta['display_state'] = "hidden"
        quick_meta['display_state_reason'] = "Hidden on opensea"
    elif quick_meta['flags']['os_safelist_state'] == "verified":
        quick_meta['display_state'] = "safe"
        quick_meta['display_state_reason'] = "Verified on opensea"
    elif quick_meta['flags']['os_safelist_state'] == "approved":
        quick_meta['display_state'] = "normal"
        quick_meta['display_state_reason'] = "Approved on opensea"
    elif get_value(data, ["erc1967_proxy_implementation"]) != None:
        proxy = get_value(data, ["erc1967_proxy_implementation"])
        if proxy in ERC1967_PROXY_WHITELIST.keys():
            quick_meta['display_state'] = "normal"
            quick_meta['display_state_reason'] = f"Known ERC1967 proxy contract: {ERC1967_PROXY_WHITELIST[proxy]}"
            quick_meta['flags']['is_whitelisted_proxy'] = True
    elif (quick_meta['flags']['code_verified'] and not quick_meta['flags']['functions_threshold']) or (not quick_meta['flags']['bytecode_threshold']):
        # either low function count or small bytecode size
        # potentially proxy or scam
        bytecode_head = get_value(data, ["byte_code_head"])
        is_known_proxy = False
        if bytecode_head:
            for k in ERC1167_BYTECODE_WHITELIST.keys():
                if bytecode_head == k:
                    quick_meta['display_state'] = "normal"
                    quick_meta['display_state_reason'] = f"Known ERC1167 proxy contract: {ERC1167_BYTECODE_WHITELIST[k]}"
                    quick_meta['flags']['is_whitelisted_proxy'] = True
                    is_known_proxy = True
                    break
        if is_known_proxy:
            pass
        elif (quick_meta['flags']['code_verified'] and not quick_meta['flags']['functions_threshold']):
            quick_meta['display_state'] = "hidden"
            quick_meta['display_state_reason'] = "< 10 functions, unlikely a standard NFT contract"
        elif not quick_meta['flags']['bytecode_threshold']:
            quick_meta['display_state'] = "hidden"
            quick_meta['display_state_reason'] = "< 1kb bytecode size, unlikely a standard NFT contract"
    elif (quick_meta['display_stats']['total_volume'] is not None) and (quick_meta['display_stats']['total_volume'] > 100):
        quick_meta['display_state'] = "normal"
        quick_meta['display_state_reason'] = "Total volume traded > 100"
    elif not (quick_meta['flags']['code_verified'] or quick_meta['flags']['approval_threshold']):
        quick_meta['display_state'] = "suspicious"
        quick_meta['display_state_reason'] = "Verified code but low setApprovalForAll count"
    return quick_meta