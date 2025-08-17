from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import pickle
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_and_split_data(**kwargs):
    """Load data and split into train/test sets"""
    logger.info("Starting data loading and splitting process...")
    
    # Load data
    data_path = '/home/rlass/airflow/dags/dataset_edit.csv'
    logger.info(f"Loading data from: {data_path}")
    
    try:
        data = pd.read_csv(data_path)
        logger.info(f"Data loaded successfully. Shape: {data.shape}")
        logger.info(f"Columns: {list(data.columns)}")
        
        # Display basic info about the dataset
        logger.info(f"Dataset info:")
        logger.info(f"- Number of rows: {len(data)}")
        logger.info(f"- Number of columns: {len(data.columns)}")
        logger.info(f"- Missing values per column:")
        for col in data.columns:
            missing_count = data[col].isnull().sum()
            if missing_count > 0:
                logger.info(f"  {col}: {missing_count}")
        
        # Assuming the last column is the target variable
        # You may need to adjust this based on your actual dataset structure
        target_column = data.columns[-1]  # or specify your target column name
        logger.info(f"Using '{target_column}' as target variable")
        
        X = data.drop(target_column, axis=1)
        y = data[target_column]
        
        logger.info(f"Features shape: {X.shape}")
        logger.info(f"Target shape: {y.shape}")
        logger.info(f"Target distribution: {y.value_counts().to_dict()}")
        
        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        logger.info("Data splitting completed:")
        logger.info(f"- Training set size: {X_train.shape[0]} samples")
        logger.info(f"- Test set size: {X_test.shape[0]} samples")
        logger.info(f"- Training set target distribution: {y_train.value_counts().to_dict()}")
        logger.info(f"- Test set target distribution: {y_test.value_counts().to_dict()}")
        
        # Save the split data to temporary files
        temp_dir = '/tmp/airflow_ml_pipeline'
        os.makedirs(temp_dir, exist_ok=True)
        
        X_train.to_csv(f'{temp_dir}/X_train.csv', index=False)
        X_test.to_csv(f'{temp_dir}/X_test.csv', index=False)
        y_train.to_csv(f'{temp_dir}/y_train.csv', index=False)
        y_test.to_csv(f'{temp_dir}/y_test.csv', index=False)
        
        logger.info("Data split and saved to temporary files successfully")
        
        # Push data info to XCom for next tasks
        return {
            'train_size': len(X_train),
            'test_size': len(X_test),
            'feature_count': X_train.shape[1],
            'target_classes': y.nunique()
        }
        
    except Exception as e:
        logger.error(f"Error in data loading and splitting: {str(e)}")
        raise

def train_model(**kwargs):
    """Train the machine learning model"""
    logger.info("Starting model training process...")
    
    # Get data info from previous task
    ti = kwargs['ti']
    data_info = ti.xcom_pull(task_ids='split_data_task')
    logger.info(f"Received data info: {data_info}")
    
    temp_dir = '/tmp/airflow_ml_pipeline'
    
    try:
        # Load training data
        logger.info("Loading training data...")
        X_train = pd.read_csv(f'{temp_dir}/X_train.csv')
        y_train = pd.read_csv(f'{temp_dir}/y_train.csv').iloc[:, 0]  # Get the first column as Series
        
        logger.info(f"Training data loaded - X_train: {X_train.shape}, y_train: {y_train.shape}")
        
        # Initialize and train the model
        logger.info("Initializing Logistic Regression model...")
        model = LogisticRegression(random_state=42, max_iter=1000)
        
        logger.info("Starting model training...")
        model.fit(X_train, y_train)
        logger.info("Model training completed successfully!")
        
        # Save the trained model
        model_path = f'{temp_dir}/trained_model.pkl'
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Model saved to: {model_path}")
        
        # Log model parameters
        logger.info("Model parameters:")
        logger.info(f"- C (regularization): {model.C}")
        logger.info(f"- Max iterations: {model.max_iter}")
        logger.info(f"- Solver: {model.solver}")
        logger.info(f"- Number of iterations performed: {model.n_iter_}")
        
        return {
            'model_path': model_path,
            'training_completed': True,
            'n_iterations': int(model.n_iter_[0]) if hasattr(model, 'n_iter_') else None
        }
        
    except Exception as e:
        logger.error(f"Error in model training: {str(e)}")
        raise

def evaluate_model(**kwargs):
    """Evaluate the trained model and show accuracy"""
    logger.info("Starting model evaluation process...")
    
    # Get info from previous tasks
    ti = kwargs['ti']
    data_info = ti.xcom_pull(task_ids='split_data_task')
    train_info = ti.xcom_pull(task_ids='train_model_task')
    
    logger.info(f"Data info: {data_info}")
    logger.info(f"Training info: {train_info}")
    
    temp_dir = '/tmp/airflow_ml_pipeline'
    
    try:
        # Load test data
        logger.info("Loading test data...")
        X_test = pd.read_csv(f'{temp_dir}/X_test.csv')
        y_test = pd.read_csv(f'{temp_dir}/y_test.csv').iloc[:, 0]  # Get the first column as Series
        
        logger.info(f"Test data loaded - X_test: {X_test.shape}, y_test: {y_test.shape}")
        
        # Load the trained model
        logger.info("Loading trained model...")
        model_path = train_info['model_path']
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        logger.info("Model loaded successfully!")
        
        # Make predictions
        logger.info("Making predictions on test data...")
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)
        
        # Calculate accuracy
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"🎯 MODEL ACCURACY: {accuracy:.4f} ({accuracy*100:.2f}%)")
        
        # Additional metrics
        logger.info("\n" + "="*50)
        logger.info("DETAILED EVALUATION RESULTS")
        logger.info("="*50)
        logger.info(f"Accuracy Score: {accuracy:.4f}")
        
        # Classification report
        logger.info("\nClassification Report:")
        class_report = classification_report(y_test, y_pred)
        logger.info(f"\n{class_report}")
        
        # Prediction distribution
        unique, counts = np.unique(y_pred, return_counts=True)
        pred_dist = dict(zip(unique, counts))
        logger.info(f"Prediction distribution: {pred_dist}")
        
        # Confidence statistics
        max_probabilities = np.max(y_pred_proba, axis=1)
        logger.info(f"Prediction confidence statistics:")
        logger.info(f"- Mean confidence: {np.mean(max_probabilities):.4f}")
        logger.info(f"- Min confidence: {np.min(max_probabilities):.4f}")
        logger.info(f"- Max confidence: {np.max(max_probabilities):.4f}")
        
        logger.info("="*50)
        logger.info("✅ MODEL EVALUATION COMPLETED SUCCESSFULLY!")
        logger.info("="*50)
        
        # Clean up temporary files
        logger.info("Cleaning up temporary files...")
        for file in ['X_train.csv', 'X_test.csv', 'y_train.csv', 'y_test.csv', 'trained_model.pkl']:
            file_path = f'{temp_dir}/{file}'
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Removed: {file}")
        
        return {
            'accuracy': float(accuracy),
            'evaluation_completed': True,
            'mean_confidence': float(np.mean(max_probabilities))
        }
        
    except Exception as e:
        logger.error(f"Error in model evaluation: {str(e)}")
        raise

# Define the DAG
with DAG(
    dag_id="machine_learning_pipeline_dag",
    description="A complete ML pipeline DAG with data splitting, training, and evaluation",
    start_date=datetime(2023, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=['machine-learning', 'pipeline'],
) as dag:
    
    # Task 1: Load and split data
    split_data_task = PythonOperator(
        task_id="split_data_task",
        python_callable=load_and_split_data,
        doc_md="Load the dataset and split it into training and testing sets"
    )
    
    # Task 2: Train the model
    train_model_task = PythonOperator(
        task_id="train_model_task",
        python_callable=train_model,
        doc_md="Train a Logistic Regression model on the training data"
    )
    
    # Task 3: Evaluate the model
    evaluate_model_task = PythonOperator(
        task_id="evaluate_model_task",
        python_callable=evaluate_model,
        doc_md="Evaluate the trained model and calculate accuracy on test data"
    )
    
    # Define task dependencies
    split_data_task >> train_model_task >> evaluate_model_task