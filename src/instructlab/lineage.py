import json
from datetime import datetime, timezone
import os
from git import Repo
import logging
import json

from lakehouse import LakehouseIceberg
from lakehouse.api import ConfigMap
from lakehouse.api import JobStats, Datasource, JobDetails
from lakehouse.assets.table import Table


# get lakehouse token from env...
LAKEHOUSE_TOKEN=os.environ.get('LAKEHOUSE_TOKEN')


if os.environ.get('LAKEHOUSE_ENV'):
    LAKEHOUSE_ENV = os.environ.get('LAKEHOUSE_ENV')
else:
    LAKEHOUSE_ENV = 'STAGING'


# define basic logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobStatsUtil:
    def __init__(self):
        # Init the Stats class and do config setup
        self.lh = None
        # self.config_path = config_path
        self.generate_data = {}
        self.model_train = {}

        # LH Setup
        self.setup_lakehouse()

    def setup_lakehouse(self):
        try:
            self.lh = LakehouseIceberg(
                config='map', 
                conf_map=ConfigMap(
                    environment=LAKEHOUSE_ENV, 
                    token=LAKEHOUSE_TOKEN))
        except:
            logging.warning("There was a problem setting up Lakehouse")

    def parse_events_json(self, data):

        # Check if empty
        if len(data) == 0:
            return False
        
        else:
            # Check if generate_data_dict key exists
            if "event_type" in data and data["event_type"] == "generate_data":
                self.generate_data = data

            # Check if model_train key exists
            elif "event_type" in data and data["event_type"] == "model_train":
                self.model_train = data

            else:
                logging.warning("Warning: No model_train was found!")
        
        return True

    def check_data_integrity(self, required_keys, data):
        # Bare minimum To fill out the JobStats
        if required_keys and all(k in data for k in required_keys):
            logging.info("Found minimum number of keys.")
            return True
        
        return False

    def post_generate_data_jobstats(self, required_keys):
        """
        Pushes the Generate Data part of the events.json to dmf JobStats
        :param generate_data_dict: The Generate Data part of the events json
        :return: Bool
        """
        if self.check_data_integrity(required_keys, self.generate_data):
            # Check the number of files_generated and push them to the table before we can proceed to push the data stat
            # @ToDo: We have to get the number of file generated and push then to the appropriate table or COS location
            #  and get the reference and then populate the targets
            # Try to save the JobStat
            try:
                stats = JobStats(
                    release_id=self.generate_data["lineage_id"],
                    job_details=JobDetails(
                        id='generate_data_' + self.generate_data["lineage_id"],
                        name='ilab-generate-data-job',
                        type='ilab',
                        category='finetuning',
                        started_at=self.generate_data["time_stamp"],
                        completed_at=self.generate_data["time_stamp"],
                        status='success',
                        owner='iLab'  # OPT
                    ),
                    sources=[Datasource(
                        name=self.generate_data["taxonomy_repo"],
                        type='dataset',
                        table='tbd',
                        snapshot_id='123456',  # OPT
                        path=[self.generate_data["taxonomy_path"]],  # OPT
                        version='rel_07',  # OPT
                        extra={"synthetic_data_generator": self.generate_data["synthetic_data_generator"],
                               "taxonomy_tree_path": self.generate_data["taxonomy_tree_path"],
                               "generator_server": self.generate_data["generator_server"],
                               "num_instructions_to_generate": self.generate_data["num_instructions_to_generate"]
                               }
                    )],
                    # @ToDo This will get populated after the "files_generated" are processed
                    targets=[Datasource(
                        name='pushshift',
                        type='dataset',
                        table='bluepile.enterprise.dedup',
                        snapshot_id='987632',
                        version='rel_07'  # OPT
                    )],
                    job_input_params={  # OPT
                    },
                    execution_stats={  # OPT
                    },
                    job_output_stats={  # OPT
                    }
                )
                self.lh.save_stats(stats)
                logging.info("Successfully added generate_data jobStat")
                return True
                
            except Exception as e:
                logging.error(e)
                
        return False

    def get_generate_data_jobstats(self):

        ### Ray Example
        # table = lh.load_table("dmf.job_stats")
        # print(table)
        # # run a query
        # scan = table.scan()
        # ds = data.from_arrow(scan.to_arrow())
        # ds.show()
        try:
            ### DuckDB example
            table = self.lh.load_table("dmf.job_stats")
            # run a query
            con = table.scan(limit=10).to_duckdb("job_stats")
            # execute SQL statements on the data in duckdb table named 'internet'
            release_id = self.generate_data["lineage_id"]
            res = con.execute(
                "select * from job_stats where release_id == '" + release_id + "' AND job_name like '%generate-data%'").df()

            cols = []
            for col in res:
                cols.append(col)

            json_docs = []
            for i in range(len(res)):
                json_doc = dict()
                
                for j in range(0, len(cols)):
                    json_doc[cols[j]] = res.loc[i, cols[j]]
                    
                json_docs.append(json_doc)

            return json_docs

        except Exception as e:
            logging.error(e)

        return None

    def post_model_train_jobstats(self, required_keys):
        """
        Pushes the Generate Data part of the events.json to dmf JobStats
        :param generate_data_dict: The Generate Data part of the events json
        :return: Bool
        """
        if self.check_data_integrity(required_keys, self.model_train):
            # Check the number of files_generated and push them to the table before we can proceed to push the data stat
            # @ToDo: We have to get the number of file generated and push then to the appropriate table or COS location
            #  and get the reference and then populate the targets
            # Try to save the JobStat
            try:
                stats = JobStats(
                    release_id=self.model_train["lineage_id"],
                    job_details=JobDetails(
                        id='model_train_' + self.model_train["lineage_id"],
                        name='ilab-model-train-job',
                        type='ilab',
                        category='finetuning',
                        started_at=self.generate_data["time_stamp"],  # train_model does not have any time stamp
                        completed_at=self.generate_data["time_stamp"],
                        status='success',
                        owner='iLab'  # OPT
                    ),
                    # @ToDo This will get populated after the retrieval of "files_generated" are processed
                    sources=[Datasource(
                        name="reference_from_files_generated_name",
                        type='dataset',
                        table='tbd',
                        snapshot_id='123456',  # OPT
                        path=["reference_from_files_generated_path"],  # OPT
                        version='rel_07',  # OPT
                    )],
                    # @ToDo: Create a list of all the trained_model_files decide if its COS or table
                    targets=[Datasource(
                        name='pushshift',
                        type='dataset',
                        table='bluepile.enterprise.dedup',
                        snapshot_id='987632',
                        version='rel_07'  # OPT
                    )],
                    job_input_params={  # OPT
                    },
                    execution_stats={  # OPT
                    },
                    job_output_stats={  # OPT
                    }
                )
                self.lh.save_stats(stats)
                logging.info("Successfully added model_train jobStat")
                return True

            except Exception as e:
                logging.error("Model Train", e)
                
        return False

    def get_model_train_jobstats(self):

        try:

            release_id = self.model_train["lineage_id"]
            partial_job_name = 'ilab-model-train-job'
            
            ### DuckDB example
            #table = self.lh.load_table("dmf.job_stats")
            # run a query
            #con = table.scan(limit=10).to_duckdb("job_stats")
            # execute SQL statements on the data in duckdb table named 'internet'
            # release_id = self.generate_data["lineage_id"]
            #query =f"select * from job_stats where release_id = '{release_id}' AND job_name like '%{partial_job_name}%'"
            #res = con.execute(query).df()

            t = Table(lh=self.lh, namespace='dmf', table_name='job_stats')
            res = t.to_pandas(rows=10, row_filter=f"release_id = '{release_id}' and job_name like '{partial_job_name}%'")

            cols = []
            for col in res:
                cols.append(col)

            json_docs = []
            for i in range(len(res)):
                json_doc = dict()
                
                for j in range(0, len(cols)):
                    json_doc[cols[j]] = res.loc[i, cols[j]]
                    
                json_docs.append(json_doc)

            return json_docs
            
        except Exception as e:
            logging.error(e)

        return None


class Lineage:
    """Lineage data structure"""
    def __init__(self, lineage_id, event_type) -> None:
        self.lineage_id = lineage_id
        self.event_type = event_type
        self.jutil = JobStatsUtil()

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

        # save to local file if desired...
        if fname:
            with open(fname, 'w') as f:
                json.dump(self.to_json(), f) 

        # push to dmf via lineage APIs
        required_keys = ["lineage_id", "event_type", "time_stamp", "taxonomy_repo", "files_generated"]

        if self.jutil.parse_events_json(self.to_json()) and self.jutil.post_generate_data_jobstats(required_keys):
            logger.info(f'DMF lineage saved successfully!')
        else:
            logger.warning(f'Could not save DMF lineage. JSON obj : {self.to_json()}')


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

        # save to local file if desired...
        if fname:
            with open(fname, 'w') as f:
                json.dump(self.to_json(), f)

        # push to dmf via lineage APIs
        required_keys = ["lineage_id", "event_type", "trained_model", "trained_model_files"]
        if self.jutil.parse_events_json(self.to_json()) and self.post_model_train_jobstats(required_keys):
            logger.info(f'DMF lineage saved successfully!')
        else:
            logger.warning(f'Could not save DMF lineage. JSON obj : {self.to_json()}')

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
