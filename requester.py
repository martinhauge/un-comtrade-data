# UN Comtrade API

import requests
import zipfile
import io

import pandas as pd
from pathlib import Path
import json

def requester(config):
	base_url = config['url']

	req_url = base_url + '/api//refs/da/bulk?parameters'

	start_year = config['start_year']
	end_year = config['end_year']

	date_list = generate_date_range(start_year, end_year)

	params = config['params']

	save_folder = Path('data', 'raw')

	if not save_folder.is_dir():
		save_folder.mkdir(parents=True)

	res = requests.get(req_url, params=params)

	if not res.url.startswith(config['url']):
		raise Exception('Authentication error:', res.url)
	if res.status_code == 200:
		data = res.json()

		for i in data:
			if i['ps'] in date_list:
				save_csv(data[i], save_folder)
	else:
		raise Exception(f'Bad response: {res.status_code}')

def save_csv(data, save_folder):
	res = requests.get(base_url + data['downloadUri'])
	if res.status_code == '200':
		z = zipfile.ZipFile(io.BytesIO(res.content))
		z.extractall(save_folder)
	else:
		raise Exception(f'Bad response:\n{data}')

def combiner(data_path, output_file, trade_flow_filter=None, classificaton_filter=None):
	data_folder =Path('data')

	output_file = config['output_file_name']

	trade_flow_filter  = config['trade_flow']
	classificaton_filter = config['classification_detail']

	df = pd.DataFrame()

	for f in data_folder.iterdir():
		if f.name.endswith('csv'):
			print(f)
			temp_df = pd.read_csv(f)
			if trade_flow_filter:
				if trade_flow_filter in ['Imports', 'Exports', 'Re-imports', 'Re-exports']:
					temp_df = temp_df.loc[temp_df['Trade Flow'] == trade_flow_filter]
			if classificaton_filter:
				if classificaton_filter in [2, 4, 6]:
					temp_df = temp_df.loc[temp_df['Commodity Code'].str.len() == classificaton_filter]

			df = df.append(temp_df)
	df.to_csv(output_file, index=False)

def generate_date_range(start_year, end_year):
	date_list = []
	for y in range(start_year, end_year + 1):
		for m in range(12):
			date_list.append(f'{y}{m + 1:02}')

def parse_config(config_path):
	with open(config_path) as f:
		config_file = f.read()

	return json.loads(config_file)

if __name__ == '__main__':

	config_file = 'config.json'

	config = parse_config(config_file)

	requester(config)
	combiner(config)


	