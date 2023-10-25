import numpy as np
import pandas as pd
import catboost as ctb
from sklearn.metrics import roc_auc_score


def train(
        train_start,
        train_end,
        val_start,
        val_end,
        df,
        num_features,
        cat_features,
        target
):
    train_start, train_end = pd.to_datetime(train_start), pd.to_datetime(train_end)
    val_start, val_end = pd.to_datetime(val_start), pd.to_datetime(val_end)

    X_train = df.loc[
        (df['ValidationDateTime'] >= train_start) & (df['ValidationDateTime'] <= train_end),
        num_features + cat_features
    ]
    y_train = df.loc[
        (df['ValidationDateTime'] >= train_start) & (df['ValidationDateTime'] <= train_end),
        target
    ]
    X_val = df.loc[
        (df['ValidationDateTime'] >= val_start) & (df['ValidationDateTime'] <= val_end),
        num_features + cat_features
    ]
    y_val = df.loc[
        (df['ValidationDateTime'] >= val_start) & (df['ValidationDateTime'] <= val_end),
        target
    ]

    X_train[cat_features] = X_train[cat_features].astype(str)
    X_val[cat_features] = X_val[cat_features].astype(str)

    model = ctb.CatBoostClassifier(
        max_depth=6,
        loss_function='Logloss',
        eval_metric='AUC:hints=skip_train~false',
        cat_features=cat_features,
        use_best_model=True,
        verbose=100,
        random_seed=42,
        auto_class_weights='SqrtBalanced'
    )

    model.fit(X_train, y_train, eval_set=(X_val, y_val), early_stopping_rounds=200)
    train_preds = np.clip(model.predict_proba(X_train)[:, 1], 0, 1)
    val_preds = np.clip(model.predict_proba(X_val)[:, 1], 0, 1)

    return {
        'model': model,
        'train_points': X_train.shape[0],
        'val_points': X_val.shape[0],
        'train_balance': (y_train.value_counts().sort_index()[0], y_train.value_counts().sort_index()[1]),
        'val_balance': (y_val.value_counts().sort_index()[0], y_val.value_counts().sort_index()[1]),
        'train_auc': roc_auc_score(y_train, train_preds),
        'val_auc': roc_auc_score(y_val, val_preds),
        'val_preds': val_preds,
        'fpr_tpr_thresh': roc_curve(y_val, val_preds)
    }