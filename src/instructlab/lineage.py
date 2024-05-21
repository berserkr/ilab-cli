import json
from datetime import datetime, timezone
import os
from git import Repo
import logging
import boto3


BUCKET='lh-test'
BASE_PATH='exp1'
s3 = boto3.client(
    's3',
    region_name='us-east',
    aws_access_key_id=os.environ['KEY_ID'],
    aws_secret_access_key=os.environ['ACCESS_KEY']
)


# define basic logger
logger = logging.getLogger(__name__)


class Lineage:
    """Lineage data structure"""
    def __init__(self, lineage_id, event_type) -> None:
        self.lineage_id = lineage_id
        self.event_type = event_type

    def to_json(self):
        json_data = dict()
        json_data['lineage_id'] = self.lineage_id
        json_data['event_type'] = self.event_type

        return json_data


class Node(Lineage):
    """Generic node lineage metadata"""
    def __init__(self, lineage_id, event_type) -> None:
        super().__init__(lineage_id, event_type)


class DataGeneration(Lineage):
    """Data ganeration class"""
    def __init__(
            self, lineage_id, synthetic_data_generator, 
            taxonomy_path, taxonomy_tree_path, generator_server, 
            files_generated, num_instructions_to_generate
            ) -> None:
        super().__init__(lineage_id, event_type="generate_data")
        self.taxonomy_repo = None
        self.synthetic_data_generator = synthetic_data_generator
        self.taxonomy_path = taxonomy_path
        self.taxonomy_tree_path = taxonomy_tree_path
        self.generator_server = generator_server
        self.files_generated = files_generated
        self.num_instructions_to_generate = num_instructions_to_generate
        self.time_stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        try:
            repo = Repo(taxonomy_path)
            self.taxonomy_repo = next(repo.remote().urls)

        except ValueError as ve:
            logger.error(f'Could not get logging due to {str(ve)}')
            
    def to_json(self):

        # call parent for basic details
        json_data = super().to_json()
        
        json_data['taxonomy_repo'] = self.taxonomy_repo
        json_data['synthetic_data_generator'] = self.synthetic_data_generator
        json_data['taxonomy_path'] = self.taxonomy_path
        json_data['taxonomy_tree_path'] = self.taxonomy_tree_path
        json_data['generator_server'] = self.generator_server
        json_data['num_instructions_to_generate'] = self.num_instructions_to_generate
        json_data['files_generated'] = self.files_generated
        json_data['time_stamp'] = self.time_stamp

        return json_data
    
    def save(self, fname=None):
        
        logger.info(f'Saving {self.to_json()}')

        if fname is None:
            fname = f'{self.lineage_id}_lineage.json'
        else:
            try:
                s3.download_file(BUCKET, f'{BASE_PATH}/{fname}', fname)

            except Exception as e:
                logger.warning(f'Could not fetch {fname} from cos due to {str(e)}')


        existing_json_data = None
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                existing_json_data = json.load(f)

        if existing_json_data is None:
            existing_json_data = dict()

        existing_json_data[self.event_type] = self.to_json()

        with open(fname, 'w') as f:
            json.dump(existing_json_data, f) 

            try:
                s3.upload_file(fname, BUCKET, f'{BASE_PATH}/{fname}')

            except Exception as e:
                logger.warning(f'Could not save {fname} in cos due to {str(e)}')   


class ModelTraining(Lineage):
    """Model training class"""
    def __init__(
            self, lineage_id, num_epochs,
            train_data, test_data, statistics, base_model, 
            trained_model, trained_model_files
            ) -> None:
        super().__init__(lineage_id, event_type="model_train")

        self.num_epochs = num_epochs
        self.train_data = train_data
        self.test_data = test_data
        self.base_model = base_model
        self.statistics = statistics
        self.trained_model = trained_model
        self.trained_model_files = trained_model_files

        self.time_stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
    def to_json(self):

        # call parent for basic details
        json_data = super().to_json()
        
        json_data['num_epochs'] = self.num_epochs
        json_data['train_data'] = self.train_data
        json_data['test_data'] = self.test_data
        json_data['base_model'] = self.base_model
        json_data['statistics'] = self.statistics
        json_data['trained_model'] = self.trained_model
        json_data['trained_model_files'] = self.trained_model_files

        return json_data

    def save(self, fname=None):

        logger.info(f'Saving {self.to_json()}')

        if fname is None:
            fname = f'{self.lineage_id}_lineage.json'
        else:
            try:
                s3.download_file(BUCKET, f'{BASE_PATH}/{fname}', fname)

            except Exception as e:
                logger.warning(f'Could not fetch {fname} from cos due to {str(e)}')

        existing_json_data = None
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                existing_json_data = json.load(f)

        if existing_json_data is None:
            existing_json_data = dict()

        existing_json_data[self.event_type] = self.to_json()

        with open(fname, 'w') as f:
            json.dump(existing_json_data, f)

            try:
                s3.upload_file(fname, BUCKET, f'{BASE_PATH}/{fname}')

            except Exception as e:
                logger.warning(f'Could not save {fname} in cos due to {str(e)}')   

def get_sha2(file_path):
    import hashlib
    
    BUFFER_SIZE = 65536 

    try:
        sha2 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(BUFFER_SIZE)
                if not data:
                    break
                sha2.update(data)
        return sha2.hexdigest()
    
    except Exception as e:
        logger.warning(f'Could not compute hash of {file_path} due to {str(e)}')
        return None

# files_with_hashes
def get_files_with_sha2(files_path):
    files = [f'{files_path}/{f}' for f in os.listdir(files_path) if os.path.isfile(f'{files_path}/{f}')]
    tuples = []
    for file in files:
        sha256hash = get_sha2(file)
        tuples.append({
            'file' : file,
            'sha256' : sha256hash
        })

    return tuples
