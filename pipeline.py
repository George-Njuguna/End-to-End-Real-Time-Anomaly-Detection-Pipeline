from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline as sklpipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import ParameterGrid
from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV, cross_validate
from sklearn.metrics import  confusion_matrix , precision_score , recall_score , f1_score, classification_report
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import os
from functions import split_func


# Modeling pipeline 
def modeling_pipe(data):
    
    # splitting variables(dependent, independent)
    X, y = split_func(data)

    # modelling
    
