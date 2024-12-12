import pandas as pd

def json_flatten(json_dict):
    def inner_flatten(x, name=''):
        single_dict = {}
        if isinstance(x, dict):
            flattened_dict = {}
            for key,value in x.items():
                flattened_x = inner_flatten(value, key)
                flattened_dict.update(flattened_x)
            single_dict.update(flattened_dict)
        elif isinstance(x, list):
            flattened_dict = {}
            flattened_x = []
            for obj in x:
                flattened_obj = inner_flatten(obj, name)
                flattened_x.append(flattened_obj)
 
            #will always be true due to the nature of the inner function return type
            if all(isinstance(y,dict) for y in flattened_x):
                for i, d in enumerate(flattened_x):
                    for k, v in d.items():
                        flattened_dict[f'{k}_{i+1}'] = v
            single_dict.update(flattened_dict)
        else:
            #single_dict[name[:-1]] = x
            single_dict[name] = x
        return single_dict
 
    return inner_flatten(json_dict)

def json_structure(f):
    main_table = pd.DataFrame()
    main_data = list()
    if isinstance(f, list):
        for protocol in f:
            main_data.append(json_flatten(protocol))
            #main_table = pd.concat([main_table, pd.json_normalize(json_flatten(protocol))])
    elif isinstance(f, dict):
        #main_table = pd.concat([main_table, pd.json_normalize(json_flatten(f[list(f.keys())[0]]))])
        main_data.append(json_flatten(f[list(f.keys())[0]]))
    #return main_table
    return main_data