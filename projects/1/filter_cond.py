def filter_cond(line_dict):
    if line_dict['if1'] = '':
        line_dict['if1'] = 0    
    cond_match = (
       (int(line_dict["if1"]) > 20) & (int(line_dict["if1"]) < 40)
    ) 
    return True if cond_match else False