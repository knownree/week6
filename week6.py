from modin import pandas as pd
import dask.dataframe as dd
import testutility as util
config_data = util.read_config_file("file.yaml")
file_type = config_data['file_type']
source_file = "./" + config_data['file_name'] + f'.{file_type}'
#print("",source_file)
df = pd.read_csv(source_file,config_data['inbound_delimiter'])
util.col_header_val(df,config_data)
print('pd:')
print("columns of files are:" ,df.columns)
print("columns of YAML are:" ,config_data['columns'])
if util.col_header_val(df,config_data)==0:
    print("pd:validation failed")
else:
    print("pd:col validation passed")

df=dd.read_csv(source_file,dtype='str')
print('Dask:')
print("columns of files are:" ,df.columns)
print("columns of YAML are:" ,config_data['columns'])
if util.col_header_val(df,config_data)==0:
    print("dask:validation failed")
else:
    print("dask:col validation passed")