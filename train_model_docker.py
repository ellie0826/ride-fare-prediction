#!/usr/bin/env python3
"""
Docker-based training script for the ride fare prediction model
"""

import os
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_training_in_docker():
    """
    Run the training process in a Docker container
    """
    logger.info("Starting model training in Docker container...")
    
    # Check if Docker is available
    try:
        subprocess.run(['docker', '--version'], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("Docker is not available. Please install Docker first.")
        return 1
    
    # Check if data file exists
    if not os.path.exists("uber.csv"):
        logger.error("Data file not found: uber.csv")
        logger.error("Please ensure the Uber dataset is available.")
        return 1
    
    try:
        # Build a temporary training image
        logger.info("Building training environment...")
        
        dockerfile_content = """
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ml_pipeline/ ./ml_pipeline/
COPY config/ ./config/
COPY uber.csv .

# Create models directory
RUN mkdir -p models

# Training script
COPY train_model_simple.py .

CMD ["python", "train_model_simple.py"]
"""
        
        # Write temporary Dockerfile
        with open("Dockerfile.training", "w") as f:
            f.write(dockerfile_content)
        
        # Create simplified training script
        training_script = """
import sys
sys.path.insert(0, '.')

from ml_pipeline.data_preprocessor import DataPreprocessor
from ml_pipeline.model_trainer import ModelTrainer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("Starting model training...")
        
        # Initialize components
        preprocessor = DataPreprocessor()
        trainer = ModelTrainer()
        
        # Load and preprocess data
        logger.info("Loading and preprocessing data...")
        df = preprocessor.load_and_preprocess_data("uber.csv")
        logger.info(f"Loaded {len(df)} records after preprocessing")
        
        # Prepare features and target
        X, y = preprocessor.prepare_features_target(df)
        logger.info(f"Feature matrix shape: {X.shape}")
        
        # Train models
        logger.info("Training models...")
        training_results = trainer.train_models(X, y)
        
        # Save best model
        logger.info("Saving best model...")
        model_path, feature_path, metadata_path = trainer.save_model(
            training_results['best_model'],
            training_results['best_model_name'],
            training_results['feature_names']
        )
        
        logger.info("=" * 50)
        logger.info("TRAINING COMPLETED SUCCESSFULLY!")
        logger.info("=" * 50)
        logger.info(f"Best model: {training_results['best_model_name']}")
        
        # Print model performance
        best_results = training_results['results'][training_results['best_model_name']]
        logger.info("Model Performance:")
        logger.info(f"  RMSE: {best_results['rmse']:.4f}")
        logger.info(f"  MAE: {best_results['mae']:.4f}")
        logger.info(f"  R²: {best_results['r2']:.4f}")
        logger.info(f"  MAPE: {best_results['mape']:.2f}%")
        
        return 0
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
"""
        
        with open("train_model_simple.py", "w") as f:
            f.write(training_script)
        
        # Build Docker image
        logger.info("Building Docker image for training...")
        build_cmd = ["docker", "build", "-f", "Dockerfile.training", "-t", "fare-prediction-trainer", "."]
        result = subprocess.run(build_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error("Failed to build Docker image:")
            logger.error(result.stderr)
            return 1
        
        # Run training in container
        logger.info("Running training in Docker container...")
        run_cmd = [
            "docker", "run", "--rm",
            "-v", f"{os.getcwd()}/models:/app/models",
            "fare-prediction-trainer"
        ]
        
        result = subprocess.run(run_cmd)
        
        # Cleanup temporary files
        try:
            os.remove("Dockerfile.training")
            os.remove("train_model_simple.py")
        except:
            pass
        
        if result.returncode == 0:
            logger.info("\n🎉 Training completed successfully!")
            logger.info("Models saved to ./models/ directory")
            logger.info("You can now start the system with: docker-compose up")
        else:
            logger.error("Training failed in Docker container")
        
        return result.returncode
        
    except Exception as e:
        logger.error(f"Docker training failed: {e}")
        return 1


def main():
    """
    Main function
    """
    logger.info("🚗 Ride Fare Prediction Model Training (Docker)")
    logger.info("=" * 50)
    
    return run_training_in_docker()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
