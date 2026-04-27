import dagshub
import mlflow

from us_visa.entity.config_entity import MlflowConfig

def setup_mlflow(config: MlflowConfig):
    """
    Initializes MlFlow tracking with dagshub
    """

    dagshub.init(
        repo_owner = config.repo_owner,
        repo_name = config.repo_name,
        mlflow = True
    )

    mlflow.set_tracking_uri(config.tracking_url)
    mlflow.set_experiment(config.experiment_name)

    mlflow.autolog(disable=True)