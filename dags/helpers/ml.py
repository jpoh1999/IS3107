from database.warehouse import *
from database.lake import *
from helpers.dummyops import start, end
from airflow.decorators import task, task_group
from database.sql import CREATE_QUERIES_ML, DROP_QUERIES_ML
from constants import MLMART_PARAMS
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import joblib
from sqlalchemy import create_engine
from constants import RANDOM_STATE, DATA_DIR

## Modelling
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import StackingClassifier ## for stack ensemble learning
from sklearn.linear_model import LogisticRegression
from sklearn.svm import  LinearSVC
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import AdaBoostClassifier, RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier


## Evaluation
from sklearn.metrics import confusion_matrix, classification_report, roc_auc_score, accuracy_score, recall_score, precision_score

## MLFlow
import mlflow
from mlflow.models import infer_signature
import mlflow.pyfunc

#region : Helper funcs
def make_dir(date : datetime) :
    """
        Create a directory for the current date
        in our local directory
    """
    dirpath = f"{DATA_DIR}/{date.strftime('%Y%m%d')}"

    os.makedirs(dirpath, exist_ok=True)

    return dirpath


def get_mlmart_connection() :
    """
        Description : A helper function to 
        connect to mlmart
    """
    hostname = MLMART_PARAMS["host"]
    password = MLMART_PARAMS["password"]
    username = MLMART_PARAMS["user"]
    database = MLMART_PARAMS["database"]

    try :
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=database, user=username, pw=password))
        connection = engine.connect()
    except Exception as e :
        print(e)
    
    return connection

def evaluate(y_test,y_pred):
    ''' Evaluation helper functions to get the classification report and confusion matrix for the given model

    Args :
      y_test : (pd.Series) : The series of actual results
      y_pred : (pd.Series) : The series of the predicted results

    '''
    print('Classification Report:')
    print(classification_report(y_test, y_pred))
    print('Confusion Matrix:')
    confusion_mat = confusion_matrix(y_test, y_pred)
    print(confusion_mat)

    # cm_display = ConfusionMatrixDisplay(confusion_matrix = confusion_mat, display_labels = [False, True])
    # cm_display.plot()
    # plt.title(f'Confusion Matrix for {modelName}')
    # plt.show()

#endregion

#region : DataPreparation
@task(task_id = "etl_machinelearning")
def etl_ml() :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : ETL Process for Dashboard DataMart
        Entire extraction and transformation done 
        using SQL Queries.
    """
    drop_create_tables(MLMART_PARAMS, CREATE_QUERIES_ML, DROP_QUERIES_ML)

@task(task_id = "one_hot_encoding")
def one_hot_encoding(threshold = 10, **context) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        
        Extract y-features and x-features for ml

        Primarily does one-hot-encoding of the categorical features
        And also generate a binary target feature based on roi

        :threshold: the amount of ROI % for the movie to be considered profitable
        :context: xcomm to pull current date

        Returns : the destpath of the X_feature
    """
    sql = """SELECT * FROM movies;""" # Selecting from raw data
    date = context["logical_date"] # get date of execution

    connection = get_mlmart_connection() # Establish a connection
    destpath = f"{make_dir(date)}/X_data.parquet" # Dest file path for parquet file
    
    df = pd.read_sql(sql=sql, con=connection) # Raw dataframe 
    
    # Adding a target column based on threshold
    y = df.roi >= threshold
    
    # Drop the description column because we decided not to do nlp 
    df.drop(columns=["description"], inplace=True) 
    
    # OHE Cateogrical cols : No need to do after t-t split because it is a simple transformation
    cat_cols = ["genres","keywords","director","cast","production_companies","production_countries","release_year"]
    
    # --------------- Get OHE Encoding for all categorical data ---------------- #
    df_genres = df.genres.str.get_dummies(sep=",").add_prefix("genre_")
    df_keywords = df.keywords.str.get_dummies(sep=",").add_prefix("keywords_")
    df_cast = df.cast.str.get_dummies(sep=",").add_prefix("cast_")
    df_production_companies = df.production_companies.str.get_dummies(sep=",").add_prefix("production_")
    df_production_countries = df.production_countries.str.get_dummies(sep=",").add_prefix("countries_")
    df_director = df.director.str.get_dummies(sep=",").add_prefix("director")
    df_release_year = df.release_year.astype(str).str.get_dummies()

    # ------------------ Convert data types to boolean ------------------- #
    df_genres = df_genres.astype(bool)
    df_keywords = df_keywords.astype(bool)
    df_cast = df_cast.astype(bool)
    df_production_companies = df_production_companies.astype(bool)
    df_production_countries = df_production_countries.astype(bool)
    df_director = df_director.astype(bool)
    df_release_year = df_release_year.astype(bool)

    # ----------------- Combine the dataframes -------------------------- # 
    combined_df = [df,df_genres, df_keywords, df_cast, df_production_companies, df_production_countries, df_director, df_release_year]

    df_combined = pd.concat(combined_df, axis=1)
    df_combined.drop(columns=cat_cols, inplace=True)
    df_combined.drop(columns=["revenue","roi"], inplace=True) ## knowing these variables directly tell us profitability of the movie so we drop
    
    # ---------------- Set the title of the movies as the indexes --------------- #
    df_combined.set_index('title', inplace=True) # set the title as the index

    df_combined.to_parquet(destpath) ## store columnar df as parquet file

    upload_blob(BUCKETNAME, destpath, destpath) # Upload a copy to gcs for data lineage

    y.to_sql("y_ml", con=connection, if_exists="replace", index=False) ## store the y_labels in sql

    return destpath

@task(task_id = "train_test_split")
def tt_split(X_filepaths : str, size : int = 0.2, **context) :
    """ 
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : Train test split pipe for data ops. 
        It separates X,y into their train, test split with test size of test_size

        :X_filepaths: the filepath of the x parquet file from previous stage
        :size: the test_size split, default = 0.2
        :context: the xcomm to pull current date
    """
    connection = get_mlmart_connection()
    read_y = """SELECT * FROM y_ml"""

    date = context["logical_date"] # Get date of execution

    X = pd.read_parquet(X_filepaths)
    y = pd.read_sql(read_y, con=connection)

    X_train_path = f"{make_dir(date)}/X_train.parquet" # Dest file path for xtrain
    X_test_path = f"{make_dir(date)}/X_test.parquet" # Dest file path for xtrain

    # Splitting the 
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = size, random_state = RANDOM_STATE)

    X_train.to_parquet(X_train_path)
    X_test.to_parquet(X_test_path)
    
    # Upload a copy to gcs for data lineage
    upload_blob(BUCKETNAME, X_train_path, X_train_path)
    upload_blob(BUCKETNAME, X_test_path, X_test_path)

    y_train.to_sql("y_train", con=connection, if_exists="replace", index=False)
    y_test.to_sql("y_test", con=connection, if_exists="replace", index=False)

    return {"X_train" : X_train_path, "X_test" : X_test_path}

@task(task_id = "scaling")
def scaling(X_filepaths : dict, **context):

    """ 
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : 
        Use MinMaxScaler of (0,1) to scale X_train and X_test

        :X_filepaths: the filepath of the x parquet file from previous stage
        :context: the xcomm to pull current date
    """

    scaler = MinMaxScaler() 
    
    date = context["logical_date"] # get the date of execution

    X_trainpath = X_filepaths["X_train"]
    X_testpath = X_filepaths["X_test"]

    X_trainscaledpath = f"{make_dir(date)}/X_train_scaled.parquet"
    X_testscaledpath = f"{make_dir(date)}/X_test_scaled.parquet"

    X_train = pd.read_parquet(X_trainpath)
    X_test = pd.read_parquet(X_testpath)

    print(X_train.head())

    X_trainscaled = scaler.fit_transform(X_train)
    X_testscaled = scaler.transform(X_test)

    pd.DataFrame(X_trainscaled).to_parquet(X_trainscaledpath)
    pd.DataFrame(X_testscaled).to_parquet(X_testscaledpath)

    upload_blob(BUCKETNAME, X_trainscaledpath, X_trainscaledpath)
    upload_blob(BUCKETNAME, X_testscaledpath, X_testscaledpath)

    return {"X_train_scaled" : X_trainscaledpath, "X_test_scaled" : X_testscaledpath}

@task(task_id = "feature_selection")
def feature_selection(X_filepaths : dict, top_x : int = 50, **context) :

    """ 
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : 
        Use RF Classifier to find top x feature_importances from the current features

        :X_filepaths: the filepath of the x parquet file from previous stage
        :top_x: the number of features we want to restrict to, default to 50.
        :context: the xcomm to pull the current date

    """

    connection = get_mlmart_connection()
    read_y = """SELECT * FROM y_train"""

    date = context["logical_date"]

    X_trainpath = X_filepaths["X_train_scaled"]
    X_testpath = X_filepaths["X_test_scaled"]

    X_trainfeaturepath = f"{make_dir(date)}/X_train_feature.parquet"
    X_testfeaturepath = f"{make_dir(date)}/X_test_feature.parquet"

    X_train = pd.read_parquet(X_trainpath)
    X_test = pd.read_parquet(X_testpath)
    y_train = pd.read_sql(read_y, con=connection)

    # Using a RandomForest Classifier to get Feature Importances
    clf = RandomForestClassifier(random_state=RANDOM_STATE)
    clf.fit(X_train, y_train)

    # Get feature importances
    importances = clf.feature_importances_

    # Create a DataFrame with feature names and importances
    feature_importances = pd.DataFrame({'feature': X_train.columns, 'importance': importances})

    # Sort the DataFrame by importance in descending order
    feature_importances = feature_importances.sort_values('importance', ascending=False)

    topx_feats = feature_importances.head(top_x)["feature"].to_list()

    X_train[topx_feats].to_parquet(X_trainfeaturepath)
    X_test[topx_feats].to_parquet(X_testfeaturepath)

    upload_blob(BUCKETNAME, X_trainfeaturepath, X_trainfeaturepath)
    upload_blob(BUCKETNAME, X_testfeaturepath, X_testfeaturepath)

    return {"X_train_feat" : X_trainfeaturepath, "X_test_feat" : X_testfeaturepath}


@task(task_id = "staging_ml")
def stage_ml(X_filepaths : list, **context) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : Used as a staging node to collect 
        all filepaths from transformation

        :X_filepaths: the list of filepaths of the x parquet file from previous stages
        :context: the xcomm to pull the current date
    """
    combined_X_train = pd.DataFrame()
    combined_X_test = pd.DataFrame()

    date = context["logical_date"] # getting the execution date

    X_trainstagepath = f"{make_dir(date)}/X_train_staging.parquet"
    X_teststagepath = f"{make_dir(date)}/X_test_staging.parquet"

    for filepath in X_filepaths:
        X_trainpath = filepath["X_train_feat"]
        X_testpath = filepath["X_test_feat"]
        X_train = pd.read_parquet(X_trainpath)
        X_test = pd.read_parquet(X_testpath)
        combined_X_train = pd.concat([combined_X_train, X_train])
        combined_X_test = pd.concat([combined_X_test, X_test])

    combined_X_train.to_parquet(X_trainstagepath)
    combined_X_test.to_parquet(X_teststagepath)

    upload_blob(BUCKETNAME, X_trainstagepath, X_trainstagepath)
    upload_blob(BUCKETNAME, X_teststagepath, X_teststagepath)

    print(combined_X_train.head())
    ti = context['ti']
    ti.xcom_push(key = "filepaths", value = {"X_train" : X_trainstagepath, "X_test" : X_teststagepath})
    
#endregion

#region : MLTraining
@task(task_id = "build_models")
def build_ensemble_models(**context) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : Build the ensemble models with the selected models
        The pre-selected models are : {svm, knn, ada, mlp}

        :context: the xcomm to pull the current date

    """
    date = context['logical_date']

    # Define the base models
    estimators = [
        ('svm', LinearSVC(random_state = RANDOM_STATE, max_iter = 2000)),
        ('knn', KNeighborsClassifier(n_neighbors= 10)),
        ('ada', AdaBoostClassifier(random_state=RANDOM_STATE,n_estimators= 6, algorithm="SAMME")),
        ('mlp', MLPClassifier(random_state = RANDOM_STATE, max_iter = 500))
    ]

    # Create the ensemble model
    ensemble = StackingClassifier(
        estimators=estimators, final_estimator=LogisticRegression(random_state = RANDOM_STATE, solver = "liblinear")
    )
    ensemblepath = f'{make_dir(date)}/stacking_classifier.pkl'
    joblib.dump(ensemble, ensemblepath)
    
    return ensemblepath

@task(task_id = "training_models")
def train_models(ensemblepath, **context) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : Train the selected ensemble models on X_train

        :ensemblepath: the path where the ensemble classifier is stored locally
        :context: xcomm to pull the current date

    """
    ti = context["ti"]
    date = context["logical_date"]

    read_y = """SELECT * FROM y_train"""
    connection = get_mlmart_connection()
    ensemble_clf = joblib.load(ensemblepath)
    fitted_ensemblepath = f'{make_dir(date)}/fitted_classifier.pkl'
    X_trainpath = ti.xcom_pull(task_ids = "machine_learning.data_preparation.staging_ml", key = "filepaths")["X_train"]

    X_train = pd.read_parquet(X_trainpath)
    y_train = pd.read_sql(read_y, con=connection)

    ensemble_clf.fit(X_train,y_train)

    joblib.dump(ensemble_clf,fitted_ensemblepath)

    return fitted_ensemblepath

@task(task_id = "evaluate_model")
def evaluate_model(ensemblepath, **context) :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin

        Description : Evaluate the model on X_test

        :X_filepaths: the list of filepaths of the x parquet file from previous stages
        :context: used to retrieve the date
    """
    
    read_y = """SELECT * FROM y_test""" # SQL statement to read y_test stored in ML sql db
    connection = get_mlmart_connection() # Creates a connection to the mlmart
    
    ti = context["ti"] # Establish xcomms to pull filepath from staging ml task
    X_test_path = ti.xcom_pull(task_ids = "machine_learning.data_preparation.staging_ml", key = "filepaths")["X_test"]
    
    # Reading test date for ml prediction
    X_test = pd.read_parquet(X_test_path)
    y_test = pd.read_sql(read_y, con=connection)
    
    # Loading model from training stage
    ensemble_clf = joblib.load(ensemblepath)

    # Predict the result
    y_preds = ensemble_clf.predict(X_test)

    #Set tracking server uri for logging
    mlflow.set_tracking_uri(uri="http://host.docker.internal:5000")
    # Create a new MLflow Experiment
    mlflow.set_experiment(f"Predicting Movies")

    # Start an MLflow run to save our evaluation result
    with mlflow.start_run():
        mlflow.autolog() # auto log parameters
        mlflow.set_tag("Testing Info", "Ensemble Models")
        signature = infer_signature(y_test, y_preds)
        acc = accuracy_score(y_true=y_test, y_pred=y_preds)
        mlflow.log_metric("Accuracy Score", acc)
        model_info = mlflow.sklearn.log_model(
                    sk_model=ensemble_clf,
                    artifact_path=f"Ensemble_classifier",
                    signature=signature,
                    input_example=X_test,
                    registered_model_name=f"Ensemble_classifier"
                )
        print(model_info.model_uri)
    
    evaluate(y_test = y_test, y_pred = y_preds)

#endregion

#region Group Tasks for ML Ops
@task_group(group_id = "data_preparation")
def data_preparation() :
    """ 
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        
        Tag: Task group for preparing data for ML Training

        Description: This task group cleans and preprocess the 
        data into machine readable formats for model training.
        Intermediate files for each processes are also stored
        locally and in the gs buckets to ensure data lineage
        and recoverability. This ensures that processes are 
        robust and resilient to outages and breakages
        in the event of a broken connection or bugs.

    """
    x_filepath = one_hot_encoding()
    x_splitpath = tt_split(x_filepath, 0.2)
    x_scaledpath = scaling(x_splitpath)
    x_featpath = feature_selection(x_scaledpath, 50)
    start() >> x_filepath >> x_splitpath >> x_scaledpath >> x_featpath >> stage_ml([x_featpath]) >> end()

@task_group(group_id = "ml_ops")
def ml_ops() :
    """
        Author : James Poh Hao
        Co-author : Wei Han, Jiayi, Shan Yi, Mei Lin
        
        Tag: Task group for ML Training

        Description : This task group builds, trains
        and evaluate the ensemble models on the cleaned
        data. This task group also integrate a connection
        to MLFlow at the end of the process to ensure 
        reproducibility and consistency in our modelling.
    """
    ensemblepath = build_ensemble_models() 
    fitted_ensemble_path = train_models(ensemblepath)
    
    start() >> ensemblepath >> fitted_ensemble_path >> evaluate_model(fitted_ensemble_path) >> end()
    
#endregion







