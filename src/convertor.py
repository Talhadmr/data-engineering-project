import pandas as pd 
import os

class Convertor:
    def __init__(self, file_path):
        self.file_path = file_path
    
    def convert_to_csv(self, output_path=None):
        if output_path is None:
            output_path = self.file_path.replace(os.path.splitext(self.file_path)[1], '.csv')

        if os.path.exists(output_path):
            print(f"File already exists: {output_path}")
            return

        file_extension = os.path.splitext(self.file_path)[1].lower()
        
        if file_extension == '.parquet':
            df = pd.read_parquet(self.file_path)
        elif file_extension in ['.xls', '.xlsx']:
            df = pd.read_excel(self.file_path)
        elif file_extension == '.json':
            df = pd.read_json(self.file_path)
        elif file_extension == '.h5':
            df = pd.read_hdf(self.file_path)
        elif file_extension == '.feather':
            df = pd.read_feather(self.file_path)
        elif file_extension == '.pkl':
            df = pd.read_pickle(self.file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        df.to_csv(output_path, index=False)
        print(f"File has been converted to csv format and saved as {output_path}")
