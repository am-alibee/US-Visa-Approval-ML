import sys
import importlib
import numpy as np
import mlflow
import mlflow.sklearn

import optuna
from optuna.pruners import MedianPruner

from sklearn.model_selection import cross_val_score
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
from sklearn.pipeline import Pipeline

from us_visa.exception import UsVisaException
from us_visa.logger import logging
from us_visa.utils.main_utils import (
    load_numpy_array_data,
    load_object,
    save_object,
    read_yaml
)

from us_visa.entity.config_entity import ModelTrainerConfig, MlflowConfig
from us_visa.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelTrainerArtifact,
    ClassificationMetricArtifact
)

from us_visa.utils.mlflow_utils import setup_mlflow


def get_class(module_name: str, class_name: str):
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


class ModelTrainer:
    def __init__(
        self, 
        data_transformation_artifact: DataTransformationArtifact, 
        config: ModelTrainerConfig,
        mlflow_config: MlflowConfig
    ):
        self.data_transformation_artifact = data_transformation_artifact
        self.config = config
        self.schema = read_yaml(config.model_config_file_path)

        self.seed = config.random_state
        np.random.seed(self.seed)

        setup_mlflow(mlflow_config)

    # sample hyperparams
    def _sample_params(self, trial, search_space):
        params = {}

        for param_name, param_cfg in search_space.items():

            if param_cfg["type"] == "int":
                params[param_name] = trial.suggest_int(
                    param_name,
                    param_cfg["low"],
                    param_cfg["high"]
                )

            elif param_cfg["type"] == "float":
                params[param_name] = trial.suggest_float(
                    param_name,
                    param_cfg["low"],
                    param_cfg["high"],
                    log=param_cfg.get("log", False)
                )

            elif param_cfg["type"] == "categorical":
                params[param_name] = trial.suggest_categorical(
                    param_name,
                    param_cfg["choices"]
                )

        return params
    
    def _inject_random_state(self, model_class, params: dict):
        try:
            if "random_state" in model_class().get_params():
                params["random_state"] = self.seed
        except Exception:
            pass
        return params

    def _optimize_model(self, model_name, model_cfg, x_train, y_train):
        def objective(trial):
            try:
                params = self._sample_params(trial, model_cfg["search_space"])

                model_class = get_class(
                    module_name=model_cfg["module"], 
                    class_name=model_cfg["class"]
                )

                params = self._inject_random_state(model_class=model_class, params=params)

                model = model_class(
                    **model_cfg.get("params", {}),
                    **params
                )

                # cross validation
                scores = cross_val_score(
                    model,
                    x_train,
                    y_train,
                    cv=self.schema["tuning"]["cv"],
                    scoring="f1_weighted",
                    n_jobs=-1
                )

                f1 = scores.mean()

                # log each trial
                with mlflow.start_run(nested=True):
                    mlflow.set_tag("model", model_name)
                    mlflow.set_tag("trial_number", trial.number)
                    mlflow.log_params(params)
                    mlflow.log_metric("cv_f1", f1)

                return f1
            
            except Exception as e:
                logging.error(f"{model_name} trial failed: {e}")
                return -0 # penalize instead of crashing
                # raise UsVisaException(e, sys)
            
        study = optuna.create_study(
            direction=self.schema["tuning"]["direction"],
            pruner=MedianPruner(),
            sampler=optuna.samplers.TPESampler(seed=self.seed)
        )

        study.optimize(
            objective,
            n_trials=self.schema["tuning"]["n_trials"]
        )

        best_params = study.best_params
        best_score = study.best_value

        logging.info(f"{model_name} best cv f1: {best_score}")

        # train model on full training data
        model_class = get_class(
            model_cfg["module"], 
            model_cfg["class"]
        )

        best_model = model_class(
            **model_cfg.get("params", {}),
            **best_params
        )

        best_model.fit(x_train, y_train)

        return best_model, best_params, best_score


    # main pipeline
    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        try:
            with mlflow.start_run(run_name="model_training"):

                logging.info("Loading transformed data")

                train_arr = load_numpy_array_data(
                    self.data_transformation_artifact.transformed_train_file_path
                )

                test_arr = load_numpy_array_data(
                    self.data_transformation_artifact.transformed_test_file_path
                )

                x_train, y_train = train_arr[:, :-1], train_arr[:, -1]
                x_test, y_test = test_arr[:, :-1], test_arr[:, -1]

                best_global_model = None
                best_global_score = -float("inf")
                best_global_params = None
                best_model_name = None

                # model loop
                for model_name, model_cfg in self.schema["models"].items():
                    logging.info(f"Optimizing {model_name}")

                    try:
                        model, params, score = self._optimize_model(
                            model_name,
                            model_cfg,
                            x_train,
                            y_train
                        )

                        mlflow.log_metric(f"{model_name}_best_cv_f1", score)

                        if score > best_global_score:
                            best_global_score = score
                            best_global_model = model
                            best_global_params = params
                            best_model_name = model_name
                    
                    except Exception as e:
                        logging.error(f"{model_name} failed entirely: {e}")
                        continue

                if best_global_model is None:
                    raise Exception("No valid model found")
                
                # enforce minimum performance
                if best_global_score < self.config.expected_f1_score:
                    raise Exception(
                        f"No model met expected F1. Best: {best_global_score}"
                    )
                
                logging.info(f"Best model selected: {best_model_name}")

                # final evaluation on test set
                y_pred = best_global_model.predict(x_test)

                metrics = {
                    "accuracy": accuracy_score(y_test, y_pred),
                    "f1": f1_score(y_test, y_pred),
                    "precision": precision_score(y_test, y_pred),
                    "recall": recall_score(y_test, y_pred)
                }

                # save model
                save_object(
                    self.config.trained_model_file_path,
                    best_global_model
                )

                # mlflow logging
                mlflow.log_params({
                    "best_model": best_model_name,
                    **best_global_params
                })

                mlflow.log_metrics(metrics)

                mlflow.sklearn.log_model(
                    best_global_model,
                    artifact_path="model"
                )

                metric_artifact = ClassificationMetricArtifact(
                    f1_score=metrics["f1"],
                    precision_score=metrics["precision"],
                    recall_score=metrics["recall"]
                )

                logging.info("Model training completed successfully")

                return ModelTrainerArtifact(
                    trained_model_file_path=self.config.trained_model_file_path,
                    metric_artifact=metric_artifact,
                    best_model_name=best_model_name,
                    best_params=best_global_params
                )
        
        except Exception as e:
            raise UsVisaException(e, sys)