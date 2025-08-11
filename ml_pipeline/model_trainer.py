import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import xgboost as xgb
import joblib
import mlflow
import mlflow.sklearn
import mlflow.xgboost
from typing import Dict, Any
import logging
import os
from datetime import datetime
import warnings

# Suppress urllib3 warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reduce MLflow logging noise
logging.getLogger("mlflow").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


class ModelTrainer:
    """
    Trains and evaluates fare prediction models
    """
    
    def __init__(self, mlflow_tracking_uri: str = "http://localhost:5001"):
        try:
            # Set shorter timeout to fail faster if MLflow is not available
            import os
            os.environ['MLFLOW_HTTP_REQUEST_TIMEOUT'] = '5'
            
            mlflow.set_tracking_uri(mlflow_tracking_uri)
            mlflow.set_experiment("ride-fare-prediction")
            
            # Test connection with a simple call
            mlflow.get_experiment_by_name("ride-fare-prediction")
            
        except Exception as e:
            logger.warning(f"MLflow setup failed: {e}. Running without MLflow tracking.")
            self.mlflow_enabled = False
        else:
            self.mlflow_enabled = True
            logger.info("MLflow tracking enabled")
        
    def train_models(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """
        Train multiple models and return the best one
        """
        logger.info("Starting model training...")
        
        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        models = {}
        results = {}
        
        # Train Random Forest
        logger.info("Training Random Forest...")
        rf_model, rf_metrics = self._train_random_forest(X_train, X_test, y_train, y_test)
        models['random_forest'] = rf_model
        results['random_forest'] = rf_metrics
        
        # Train XGBoost
        logger.info("Training XGBoost...")
        xgb_model, xgb_metrics = self._train_xgboost(X_train, X_test, y_train, y_test)
        models['xgboost'] = xgb_model
        results['xgboost'] = xgb_metrics
        
        # Select best model based on RMSE
        best_model_name = min(results.keys(), key=lambda k: results[k]['rmse'])
        best_model = models[best_model_name]
        
        logger.info(f"Best model: {best_model_name}")
        logger.info(f"Best RMSE: {results[best_model_name]['rmse']:.4f}")
        
        return {
            'best_model': best_model,
            'best_model_name': best_model_name,
            'all_models': models,
            'results': results,
            'feature_names': list(X.columns)
        }
    
    def _train_random_forest(self, X_train, X_test, y_train, y_test):
        """
        Train Random Forest model
        """
        # Model parameters
        params = {
            'n_estimators': 100,
            'max_depth': 15,
            'min_samples_split': 5,
            'min_samples_leaf': 2,
            'random_state': 42,
            'n_jobs': -1
        }
        
        if self.mlflow_enabled:
            with mlflow.start_run(run_name="random_forest"):
                # Log parameters
                mlflow.log_params(params)
                
                # Train model
                model = RandomForestRegressor(**params)
                model.fit(X_train, y_train)
                
                # Make predictions
                y_pred = model.predict(X_test)
                
                # Calculate metrics
                metrics = self._calculate_metrics(y_test, y_pred)
                
                # Log metrics
                mlflow.log_metrics(metrics)
                
                # Log model
                mlflow.sklearn.log_model(model, "model")
                
                # Log feature importance
                feature_importance = pd.DataFrame({
                    'feature': X_train.columns,
                    'importance': model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                mlflow.log_text(feature_importance.to_string(), "feature_importance.txt")
        else:
            # Train model without MLflow
            model = RandomForestRegressor(**params)
            model.fit(X_train, y_train)
            
            # Make predictions
            y_pred = model.predict(X_test)
            
            # Calculate metrics
            metrics = self._calculate_metrics(y_test, y_pred)
            
            logger.info(f"Random Forest metrics: {metrics}")
        
        return model, metrics
    
    def _train_xgboost(self, X_train, X_test, y_train, y_test):
        """
        Train XGBoost model
        """
        # Model parameters
        params = {
            'n_estimators': 100,
            'max_depth': 6,
            'learning_rate': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'n_jobs': -1
        }
        
        if self.mlflow_enabled:
            with mlflow.start_run(run_name="xgboost"):
                # Log parameters
                mlflow.log_params(params)
                
                # Train model
                model = xgb.XGBRegressor(**params)
                model.fit(X_train, y_train)
                
                # Make predictions
                y_pred = model.predict(X_test)
                
                # Calculate metrics
                metrics = self._calculate_metrics(y_test, y_pred)
                
                # Log metrics
                mlflow.log_metrics(metrics)
                
                # Log model
                mlflow.xgboost.log_model(model, "model")
                
                # Log feature importance
                feature_importance = pd.DataFrame({
                    'feature': X_train.columns,
                    'importance': model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                mlflow.log_text(feature_importance.to_string(), "feature_importance.txt")
        else:
            # Train model without MLflow
            model = xgb.XGBRegressor(**params)
            model.fit(X_train, y_train)
            
            # Make predictions
            y_pred = model.predict(X_test)
            
            # Calculate metrics
            metrics = self._calculate_metrics(y_test, y_pred)
            
            logger.info(f"XGBoost metrics: {metrics}")
        
        return model, metrics
    
    def _calculate_metrics(self, y_true, y_pred):
        """
        Calculate evaluation metrics
        """
        return {
            'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
            'mae': mean_absolute_error(y_true, y_pred),
            'r2': r2_score(y_true, y_pred),
            'mape': np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        }
    
    def save_model(self, model, model_name: str, feature_names: list, model_dir: str = "models"):
        """
        Save model to disk and register in MLflow
        """
        os.makedirs(model_dir, exist_ok=True)
        
        # Save model
        model_path = os.path.join(model_dir, f"{model_name}_model.joblib")
        joblib.dump(model, model_path)
        
        # Save feature names
        feature_path = os.path.join(model_dir, f"{model_name}_features.joblib")
        joblib.dump(feature_names, feature_path)
        
        # Save metadata
        metadata = {
            'model_name': model_name,
            'feature_names': feature_names,
            'trained_at': datetime.now().isoformat(),
            'model_path': model_path,
            'feature_path': feature_path
        }
        
        metadata_path = os.path.join(model_dir, f"{model_name}_metadata.joblib")
        joblib.dump(metadata, metadata_path)
        
        logger.info(f"Model saved to {model_path}")
        
        # Register model in MLflow if enabled
        if self.mlflow_enabled:
            try:
                with mlflow.start_run():
                    if 'xgb' in model_name.lower():
                        mlflow.xgboost.log_model(model, "model", registered_model_name=f"fare_prediction_{model_name}")
                    else:
                        mlflow.sklearn.log_model(model, "model", registered_model_name=f"fare_prediction_{model_name}")
            except Exception as e:
                logger.warning(f"Failed to register model in MLflow: {e}")
        
        return model_path, feature_path, metadata_path


def main():
    """
    Main training pipeline
    """
    from data_preprocessor import DataPreprocessor
    
    # Initialize components
    preprocessor = DataPreprocessor()
    trainer = ModelTrainer()
    
    # Load and preprocess data
    logger.info("Loading and preprocessing data...")
    df = preprocessor.load_and_preprocess_data("data/uber.csv")
    
    # Prepare features and target
    X, y = preprocessor.prepare_features_target(df)
    
    # Train models
    training_results = trainer.train_models(X, y)
    
    # Save best model
    model_path, feature_path, metadata_path = trainer.save_model(
        training_results['best_model'],
        training_results['best_model_name'],
        training_results['feature_names']
    )
    
    logger.info("Training completed successfully!")
    logger.info(f"Best model: {training_results['best_model_name']}")
    logger.info(f"Model saved to: {model_path}")
    
    return training_results


if __name__ == "__main__":
    main()
