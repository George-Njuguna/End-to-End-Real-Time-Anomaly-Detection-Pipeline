from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline as pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import ParameterGrid
from sklearn.model_selection import train_test_split, StratifiedKFold, GridSearchCV, cross_validate
from sklearn.metrics import  confusion_matrix , precision_score , recall_score , f1_score, classification_report
from dotenv import load_dotenv
from tqdm.notebook import tqdm
import pandas as pd
import numpy as np
import os
from functions import split_func


# Modeling pipeline 
def modeling_pipe(data):
    
    # splitting variables(dependent, independent)
    X, y = split_func(data)

     # Splitting the dataset(train, test)
    X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
    )
    try:
        # modelling
        # Scalling ammount column
        scaled_col = ['ammount']

        processor = ColumnTransformer(
            transformers = [
                ('scale_column', StandardScaler(), scaled_col )
            ],
            remainder = 'passthrough'
        )

        # pipeline
        pipe = pipeline([
            ('processor', processor),
            ('clf', LogisticRegression( max_iter=1000 ))
        ])

        param_grid = [
            {'clf__penalty':[ 'l2', None],
            'clf__C' : [0.1,1,10],
            'clf__solver': ['lbfgs']
        }
        ]

        # cross validation
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

        grid = GridSearchCV(
            estimator = pipe,
            param_grid = param_grid,
            scoring = 'f1',
            cv = skf,
            n_jobs = 1,
            verbose = 0

        )

        param_list = list(ParameterGrid(param_grid))

        print("Running GridSearchCV with progress bar...")
        for params in tqdm(param_list, desc="GridSearch Progress"):
            grid.set_params(param_grid={k: [v] for k, v in params.items()})
            grid.fit(X_train, y_train)


        # Evaluation
        best_model1= grid.best_estimator_

        y_pred1 = best_model1.predict(X_test)

        precision = precision_score(y_test, y_pred1)
        recall = recall_score(y_test, y_pred1)
        f1 = f1_score(y_test, y_pred1)
        report = classification_report(y_test, y_pred1, output_dict=True)
        report_df = pd.DataFrame(report).transpose()

        print(report_df)

        return precision, recall, f1 
    
    except Exception as e:
        print(" ERROR : IN THE MODELLING PIPELINE : ", e)

