# UN Comtrade API

import requests
import zipfile
import io

import pandas as pd
from pathlib import Path
import json

def requester(config):
	base_url = config['url']

	req_url = base_url + '/api//refs/da/bulk?'

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
		print(res.url)
		data = res.json()

		for i in data:
			if i['ps'] in date_list:
				if not Path(save_folder, i['name'].replace('zip', 'csv')) in save_folder.iterdir():
					save_csv(i, save_folder, base_url)
				else:
					print(i['name'], 'already exists. Skipping...')
	else:
		raise Exception(f'Bad response: {res.status_code}')

def save_csv(data, save_folder, url):
	res = requests.get(url + data['downloadUri'])
	if res.status_code == 200:
		
		print('Saving', data['name'])
		z = zipfile.ZipFile(io.BytesIO(res.content))
		z.extractall(save_folder)
		
	else:
		raise Exception(f'Bad response: {res.status_code} ({res.url})\n{data}')

def combiner(config):
	data_folder =Path('data', 'raw')

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

	df = df.sort_values(by='Period')
	df.to_csv(output_file, index=False)
	print('Data saved to:', output_file)

def generate_date_range(start_year, end_year):
	date_list = []
	for y in range(start_year, end_year + 1):
		for m in range(12):
			date_list.append(f'{y}{m + 1:02}')

	return date_list

def parse_config(config_path):
	with open(config_path) as f:
		config_file = f.read()

	return json.loads(config_file)

if __name__ == '__main__':

	config_file = 'config.json'

	config = parse_config(config_file)

	requester(config)
	combiner(config)


	