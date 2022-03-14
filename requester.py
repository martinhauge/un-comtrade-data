# UN Comtrade API Bulk data extraction 

import requests
import zipfile
import io

from pathlib import Path
import pandas as pd
import json

class DataRequester:
	def __init__(self, config):
		if not check_parameters(config):
			raise Exception('Configuration parameters could not be loaded. Check config file.')
		
		self.params = config['params']
		self.url = config['url']
		self.request_url = self.url + '/api//refs/da/bulk?'
		self.start_year = config['start_year']
		self.end_year = config['end_year']

		self.date_range = generate_date_range(self.start_year, self.end_year, self.params['freq'])

		self.save_folder = Path(config['data_folder'])

		if not self.save_folder.is_dir():
			self.save_folder.mkdir(parents=True)
		


	def call_api(self):
		res = requests.get(self.request_url, params=self.params)

		if not res.url.startswith(self.url):
			raise Exception('Authentication error: Make sure access through IP address is granted and check URL in config file. Returned URL:', res.url)
		if res.status_code == 200:
			self.response_data = res.json()
		else:
			raise Exception('Unsuccessful request:', res.status_code)

	def request_data(self):
		for item in self.response_data:
			if item['ps'] in self.date_range:
				file_path = Path(self.save_folder, item['name'].replace('zip', 'csv'))
				if not file_path.exists(): # in self.save_folder.iterdir():
					self.save_csv(item, file_path)
				else:
					print(item['name'], 'already extracted. Skipping...')

	def save_csv(self, item, file_path):
		res = requests.get(self.url + item['downloadUri'])
		if res.status_code == 200:
			print('Saving', item['name'])
			z = zipfile.ZipFile(io.BytesIO(res.content))
			z.extractall(self.save_folder)
		else:
			raise Exception(f'Unsuccessful request: {res.status_code} ({res.url})\n{item}')

class DataMerger:
	def __init__(self, config):
		self.data_folder = Path(config['data_folder'])
		self.output_file = config['output_file_name']

		self.trade_flow_filter = config['trade_flow']
		self.classification_filter = config['detailed_classification']

		self.df = pd.DataFrame()

	def merge_files(self):
		for f in self.data_folder.iterdir():
			if f.name.endswith('csv'):
				temp_df = pd.read_csv(f)
				if self.trade_flow_filter in ['Imports', 'Exports', 'Re-imports', 'Re-exports']:
					temp_df = temp_df.loc[temp_df['Trade Flow'] == self.trade_flow_filter]
				if self.classification_filter in [2, 4, 6]:
					temp_df = temp_df.loc[temp_df['Commodity Code'].str.len() == self.classification_filter]
				self.df = self.df.append(temp_df)

	def save_data(self):
		self.df = self.df.sort_values(by='Period')
		self.df.to_csv(self.output_file, index=False)


def check_parameters(config):
	return isinstance(config['url'], str) and \
	isinstance(config['start_year'], int) and \
	isinstance(config['end_year'], int) and \
	isinstance(config['output_file_name'], str) and \
	isinstance(config['trade_flow'], str) and \
	isinstance(config['detailed_classification'], int)	

def generate_date_range(start_year, end_year, freq):
	date_list = []
	for y in range(start_year, end_year + 1):
		if freq == 'M':
			for m in range(12):
				date_list.append(f'{y}{m + 1:02}')
		else:
			date_list.append(y)

	return date_list

def parse_config(config_path):
	with open(config_path) as f:
		config_file = f.read()

	return json.loads(config_file)

def main(config_file):

	config = parse_config(config_file)

	#data_folder = Path('data', 'raw') # TODO: Add to config

	requester = DataRequester(config)
	requester.call_api()
	requester.request_data()
	
	merger = DataMerger(config)
	merger.merge_files()
	merger.save_data()
	

if __name__ == '__main__':
	config_file = 'config.json'

	main(config_file)
