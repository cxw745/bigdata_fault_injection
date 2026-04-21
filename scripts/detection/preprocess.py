#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据预处理模块
负责对收集的指标数据进行清洗、标准化和预处理
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os


def load_metrics_from_csv(csv_path):
    """从CSV文件加载指标数据"""
    df = pd.read_csv(csv_path)
    return df


def load_metrics_from_directory(directory):
    """从目录加载所有节点的指标数据"""
    all_data = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            node_name = filename.split('_')[0]
            filepath = os.path.join(directory, filename)
            all_data[node_name] = load_metrics_from_csv(filepath)
    return all_data


def handle_missing_values(df, method='forward'):
    """
    处理缺失值
    method: 'forward', 'backward', 'mean', 'median', 'drop'
    """
    if method == 'forward':
        return df.fillna(method='ffill')
    elif method == 'backward':
        return df.fillna(method='bfill')
    elif method == 'mean':
        return df.fillna(df.mean())
    elif method == 'median':
        return df.fillna(df.median())
    elif method == 'drop':
        return df.dropna()
    return df


def normalize_data(df, method='minmax'):
    """
    标准化数据
    method: 'minmax', 'zscore'
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if method == 'minmax':
        from sklearn.preprocessing import MinMaxScaler
        scaler = MinMaxScaler()
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    elif method == 'zscore':
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    
    return df


def remove_outliers(df, method='iqr', threshold=3):
    """
    移除异常值
    method: 'iqr', 'zscore'
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if method == 'iqr':
        Q1 = df[numeric_cols].quantile(0.25)
        Q3 = df[numeric_cols].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        return df[~((df[numeric_cols] < lower_bound) | (df[numeric_cols] > upper_bound)).any(axis=1)]
    elif method == 'zscore':
        z_scores = np.abs((df[numeric_cols] - df[numeric_cols].mean()) / df[numeric_cols].std())
        return df[z_scores < threshold].copy()
    
    return df


def add_time_features(df, time_column='timestamp'):
    """添加时间特征"""
    if time_column in df.columns:
        df[time_column] = pd.to_datetime(df[time_column])
        df['hour'] = df[time_column].dt.hour
        df['day_of_week'] = df[time_column].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    return df


def add_rolling_features(df, columns, window=5):
    """添加滚动统计特征"""
    for col in columns:
        if col in df.columns:
            df[f'{col}_rolling_mean'] = df[col].rolling(window=window, min_periods=1).mean()
            df[f'{col}_rolling_std'] = df[col].rolling(window=window, min_periods=1).std()
            df[f'{col}_rolling_max'] = df[col].rolling(window=window, min_periods=1).max()
            df[f'{col}_rolling_min'] = df[col].rolling(window=window, min_periods=1).min()
    return df


def add_diff_features(df, columns):
    """添加差分特征"""
    for col in columns:
        if col in df.columns:
            df[f'{col}_diff'] = df[col].diff()
            df[f'{col}_pct_change'] = df[col].pct_change()
    return df


def preprocess_pipeline(input_data, config=None):
    """
    完整预处理流程
    """
    if config is None:
        config = {
            'handle_missing': 'forward',
            'normalize': 'minmax',
            'remove_outliers': True,
            'add_time_features': True,
            'add_rolling': True,
            'add_diff': True
        }
    
    df = input_data.copy()
    
    # 处理缺失值
    if config.get('handle_missing'):
        df = handle_missing_values(df, config['handle_missing'])
    
    # 移除异常值
    if config.get('remove_outliers'):
        df = remove_outliers(df)
    
    # 标准化
    if config.get('normalize'):
        df = normalize_data(df, config['normalize'])
    
    # 添加时间特征
    if config.get('add_time_features'):
        df = add_time_features(df)
    
    # 添加滚动特征
    if config.get('add_rolling'):
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            df = add_rolling_features(df, numeric_cols[:5], window=5)
    
    # 添加差分特征
    if config.get('add_diff'):
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            df = add_diff_features(df, numeric_cols[:5])
    
    # 填充由特征工程产生的新NaN值
    df = handle_missing_values(df, 'forward')
    df = handle_missing_values(df, 'backward')
    
    return df


def main():
    """测试预处理流程"""
    print("数据预处理模块")
    print("=" * 50)
    print("支持的功能：")
    print("1. 加载CSV指标数据")
    print("2. 处理缺失值")
    print("3. 数据标准化")
    print("4. 异常值移除")
    print("5. 添加时间特征")
    print("6. 添加滚动统计特征")
    print("7. 添加差分特征")
    print("8. 完整预处理流程")


if __name__ == "__main__":
    main()
